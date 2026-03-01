/* ═══════════════════════════════════════════════════════════════
   app.js — Pareto Contract Data Merger
   3-step flow: Upload → Map & Select → Master Sheet + Append
   ═══════════════════════════════════════════════════════════════ */

'use strict';

// ── State ───────────────────────────────────────────────────────
let uploadData = null;       // POST /api/upload response
let masterData = null;       // POST /api/merge response
let appendUploadData = null; // POST /api/append/upload response
let selectedSheets = new Set();
let appendSelectedSheets = new Set();
let jobId = null;

// ── DOM refs ────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const sections = ['s-upload', 's-loading', 's-mapping', 's-master'];

// ── Utilities ───────────────────────────────────────────────────
const esc = (s) => {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
};
const trunc = (s, n) => (s && s.length > n ? s.slice(0, n) + '…' : s || '');
const fmt = (n) => (n == null ? '—' : typeof n === 'number' ? n.toLocaleString() : String(n));

// ── Section management ──────────────────────────────────────────
function showSection(id) {
    sections.forEach((s) => $(s).classList.toggle('hidden', s !== id));
}

function setStep(n) {
    document.querySelectorAll('.step').forEach((el) => {
        const s = parseInt(el.dataset.s);
        el.classList.toggle('active', s === n);
        el.classList.toggle('done', s < n);
    });
}

// ── Upload ──────────────────────────────────────────────────────
function initUpload() {
    const zone = $('drop-zone');
    const input = $('file-input');

    zone.addEventListener('click', () => input.click());
    zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('drag-over');
        if (e.dataTransfer.files.length) doUpload(e.dataTransfer.files[0]);
    });
    input.addEventListener('change', () => { if (input.files.length) doUpload(input.files[0]); });
}

async function doUpload(file) {
    showSection('s-loading');
    setStep(1);

    const status = $('loading-status');
    const bar = $('loading-progress');
    status.textContent = `Analyzing ${file.name}…`;
    bar.style.width = '30%';

    const fd = new FormData();
    fd.append('file', file);

    try {
        bar.style.width = '60%';
        const resp = await fetch('/api/upload', { method: 'POST', body: fd });
        bar.style.width = '90%';

        if (!resp.ok) {
            const err = await resp.json();
            status.textContent = `Error: ${err.detail || 'Upload failed'}`;
            bar.style.background = 'var(--red)';
            return;
        }

        uploadData = await resp.json();
        jobId = uploadData.job_id;
        bar.style.width = '100%';

        setTimeout(() => {
            setStep(2);
            showSection('s-mapping');
            renderSheetSummary();
            buildMappingTable();
        }, 400);
    } catch (err) {
        status.textContent = `Failed: ${err.message}`;
    }
}

// ── Sheet Summary Chips ─────────────────────────────────────────
function renderSheetSummary() {
    const wrap = $('sheet-summary');
    selectedSheets = new Set();
    if (uploadData.sheets) {
        uploadData.sheets.forEach((s) => selectedSheets.add(s.sheet_name));
    }
    _rebuildSheetChips(wrap, uploadData.sheets, selectedSheets, false);
}

function _rebuildSheetChips(wrap, sheets, selected, isAppend) {
    if (!wrap) return;
    const total = (sheets || []).length;
    let html = '';

    if (total > 1) {
        const prefix = isAppend ? 'append-' : '';
        html += `<div class="sheet-select-actions">
            <span class="sheet-select-label">Select sheets to include:</span>
            <button class="sheet-select-link" data-action="${prefix}select-all">Select All</button>
            <span class="sheet-select-sep">·</span>
            <button class="sheet-select-link" data-action="${prefix}deselect-all">Deselect All</button>
        </div>`;
    }

    html += '<div class="sheet-chips-row">';
    (sheets || []).forEach((s) => {
        const isSelected = selected.has(s.sheet_name);
        const cls = isSelected ? 'selected' : 'deselected';
        const check = isSelected ? '✓' : '';
        const dataAttr = isAppend ? 'data-append-sheet' : 'data-sheet';
        html += `<div class="sheet-chip ${cls}" ${dataAttr}="${esc(s.sheet_name)}" title="Click to ${isSelected ? 'exclude' : 'include'}">
            <span class="sheet-check">${check}</span>
            ${esc(s.sheet_name)}
            <span class="muted">${fmt(s.row_count)} rows · ${fmt(s.column_count)} cols</span>
        </div>`;
    });
    html += '</div>';
    wrap.innerHTML = html;

    // Attach listeners
    _initChipListeners(wrap, sheets, selected, isAppend);
}

function _initChipListeners(wrap, sheets, selected, isAppend) {
    const chipSelector = isAppend ? '[data-append-sheet]' : '[data-sheet]';
    const dataKey = isAppend ? 'appendSheet' : 'sheet';

    wrap.querySelectorAll(chipSelector).forEach((chip) => {
        chip.onclick = () => {
            const name = chip.dataset[dataKey];
            if (selected.has(name)) selected.delete(name);
            else selected.add(name);
            _rebuildSheetChips(wrap, sheets, selected, isAppend);
            if (!isAppend) buildMappingTable();
            else buildAppendMappingTable();
        };
    });

    wrap.querySelectorAll('[data-action]').forEach((btn) => {
        btn.onclick = () => {
            const action = btn.dataset.action;
            if (action.includes('select-all')) {
                sheets.forEach((s) => selected.add(s.sheet_name));
            } else {
                selected.clear();
            }
            _rebuildSheetChips(wrap, sheets, selected, isAppend);
            if (!isAppend) buildMappingTable();
            else buildAppendMappingTable();
        };
    });
}

// ── Mapping Table ───────────────────────────────────────────────

// The target field options for the dropdown (dynamically built from columns)
function getTargetOptions(existingMasterCols) {
    // Build a set of unique column names from the current upload
    const uniqueCols = new Set();
    (uploadData?.sheets || []).forEach((s) => {
        s.columns.forEach((c) => uniqueCols.add(c));
    });

    // If we have existing master columns, include those too
    if (existingMasterCols) {
        existingMasterCols.forEach((c) => uniqueCols.add(c));
    }

    // Key columns always available
    const keyOpts = ['account_id', 'close_date'];
    const allCols = [...keyOpts];

    // Add unique cols that aren't already key opts
    uniqueCols.forEach((c) => {
        const lower = c.toLowerCase().trim();
        if (!keyOpts.includes(lower)) {
            allCols.push(c);
        }
    });

    return allCols;
}

function buildMappingTable() {
    const tbody = $('mapping-tbody');
    const sheets = (uploadData?.sheets || []).filter((s) => selectedSheets.has(s.sheet_name));

    // Collect unique columns from selected sheets + samples
    const sampleMap = {};
    const colSet = new Set();
    sheets.forEach((s) => {
        s.columns.forEach((c) => {
            colSet.add(c);
            if (!sampleMap[c] && s.sample_values[c]) {
                sampleMap[c] = s.sample_values[c];
            }
        });
    });

    const allCols = [...colSet];
    const targetOptions = getTargetOptions(null);

    const optionsHtml = `<option value="">— skip —</option>` +
        targetOptions.map((f) => {
            const isKey = f === 'account_id' || f === 'close_date';
            return `<option value="${esc(f)}">${esc(f)}${isKey ? ' ★' : ''}</option>`;
        }).join('');

    let rows = '';
    if (allCols.length === 0) {
        rows = `<tr><td colspan="3" style="text-align:center;padding:24px;color:var(--text-sec)">No sheets selected</td></tr>`;
    }

    for (const rawCol of allCols) {
        const samples = (sampleMap[rawCol] || []).slice(0, 6);
        let samplesHtml = '<span class="muted">—</span>';
        if (samples.length) {
            samplesHtml = `<div class="samples-wrap">${samples.map((s) =>
                `<span class="sample-pill">${esc(trunc(String(s), 36))}</span>`
            ).join('')}</div>`;
        }

        rows += `<tr>
            <td class="col-name">${esc(rawCol)}</td>
            <td>${samplesHtml}</td>
            <td><select class="mapping-select" data-raw="${esc(rawCol)}">${optionsHtml}</select></td>
        </tr>`;
    }

    tbody.innerHTML = rows;

    // Auto-detect: try to pre-select mappings
    document.querySelectorAll('#mapping-tbody .mapping-select').forEach((sel) => {
        const raw = sel.dataset.raw.toLowerCase().trim();
        // Try to auto-map account_id
        if (raw.includes('account') && raw.includes('id') || raw === 'accountid' || raw === 'account_id') {
            sel.value = 'account_id';
        }
        // Try to auto-map close_date
        else if (raw.includes('close') && raw.includes('date') || raw === 'closedate' || raw === 'close_date') {
            sel.value = 'close_date';
        }
        // Self-map other columns by default
        else {
            // Check if the raw column name exists in target options
            const exactMatch = targetOptions.find((t) => t === sel.dataset.raw);
            if (exactMatch) {
                sel.value = exactMatch;
            }
        }
        sel.addEventListener('change', updateMappingCounts);
    });

    updateMappingCounts();
}

function updateMappingCounts() {
    const selects = document.querySelectorAll('#mapping-tbody .mapping-select');
    let mapped = 0, skipped = 0;
    let hasAccountId = false, hasCloseDate = false;

    selects.forEach((s) => {
        if (s.value) {
            mapped++;
            if (s.value === 'account_id') hasAccountId = true;
            if (s.value === 'close_date') hasCloseDate = true;

            // Visual feedback for key column mapping
            s.classList.toggle('required-mapped', s.value === 'account_id' || s.value === 'close_date');
            s.classList.remove('required-missing');
        } else {
            skipped++;
            s.classList.remove('required-mapped', 'required-missing');
        }
    });

    $('matched-count').textContent = `${mapped} mapped`;
    $('unmapped-count').textContent = `${skipped} skipped`;

    // Enable/disable merge button based on required fields
    const mergeBtn = $('merge-btn');
    if (hasAccountId && hasCloseDate) {
        mergeBtn.disabled = false;
        mergeBtn.title = '';
    } else {
        mergeBtn.disabled = true;
        const missing = [];
        if (!hasAccountId) missing.push('account_id');
        if (!hasCloseDate) missing.push('close_date');
        mergeBtn.title = `Map these required columns first: ${missing.join(', ')}`;
    }
}

// ── Merge ───────────────────────────────────────────────────────
async function doMerge() {
    showSection('s-loading');
    const status = $('loading-status');
    const bar = $('loading-progress');
    status.textContent = 'Merging sheets…';
    bar.style.width = '30%';
    bar.style.background = '';

    // Gather mappings
    const mappings = {};
    document.querySelectorAll('#mapping-tbody .mapping-select').forEach((sel) => {
        mappings[sel.dataset.raw] = sel.value || null;
    });

    try {
        bar.style.width = '60%';
        const resp = await fetch('/api/merge', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: jobId,
                selected_sheets: [...selectedSheets],
                mappings: mappings,
            }),
        });

        bar.style.width = '90%';

        if (!resp.ok) {
            const err = await resp.json();
            if (resp.status === 409) {
                // Conflict error
                showSection('s-mapping');
                setStep(2);
                showError(err.detail);
                return;
            }
            status.textContent = `Error: ${err.detail || 'Merge failed'}`;
            bar.style.background = 'var(--red)';
            return;
        }

        masterData = await resp.json();
        bar.style.width = '100%';

        setTimeout(() => {
            setStep(3);
            showSection('s-master');
            renderMaster();
        }, 400);
    } catch (err) {
        status.textContent = `Failed: ${err.message}`;
    }
}

// ── Master Sheet ────────────────────────────────────────────────
function renderMaster() {
    const data = masterData;
    const summary = data.summary || {};

    // Banner
    $('banner-icon').textContent = '✅';
    $('banner-title').textContent = 'Master Sheet Ready';
    $('banner-sub').textContent = `${fmt(summary.row_count)} rows · ${fmt(summary.column_count)} columns · ${data.sheets_merged || '?'} sheets merged`;

    // KPIs
    const kpis = [
        { val: fmt(summary.row_count), label: 'Total Rows' },
        { val: fmt(summary.column_count), label: 'Columns' },
        { val: data.sheets_merged || '—', label: 'Sheets Merged' },
    ];

    // Key coverage
    const keyCov = summary.key_coverage || {};
    for (const k of ['account_id', 'close_date']) {
        if (keyCov[k]) {
            kpis.push({ val: `${keyCov[k].pct}%`, label: `${k} Fill` });
        }
    }

    $('kpi-grid').innerHTML = kpis.map((k) =>
        `<div class="kpi"><div class="kpi-val">${k.val}</div><div class="kpi-label">${k.label}</div></div>`
    ).join('');

    renderDataPreview(data);
    renderQuality(data);
    setupDownloads();
}

function renderDataPreview(data) {
    const cols = data.columns || [];
    const rows = data.preview || [];
    const total = data.summary?.row_count ?? rows.length;

    $('preview-count').textContent = total === 0
        ? 'No rows in output'
        : `Showing ${rows.length} of ${fmt(total)} rows`;

    if (cols.length === 0) {
        $('data-thead').innerHTML = '<tr><th>—</th></tr>';
        $('data-tbody').innerHTML = '<tr><td class="muted">No columns</td></tr>';
        return;
    }

    $('data-thead').innerHTML = `<tr>${cols.map((c) => {
        const isKey = c === 'account_id' || c === 'close_date';
        return `<th>${esc(c)}${isKey ? ' ★' : ''}</th>`;
    }).join('')}</tr>`;

    $('data-tbody').innerHTML = rows.length === 0
        ? `<tr><td colspan="${cols.length}" class="muted" style="text-align:center;padding:24px">No preview rows</td></tr>`
        : rows.map((row) =>
            `<tr>${cols.map((c) => `<td title="${esc(fmt(row[c]))}">${esc(trunc(fmt(row[c]), 40))}</td>`).join('')}</tr>`
        ).join('');
}

function renderQuality(data) {
    const ringEl = $('quality-ring');
    const barsEl = $('quality-bars');
    if (!ringEl || !barsEl) return;

    const cols = data.columns || [];
    const rows = data.preview || [];

    if (cols.length === 0 || rows.length === 0) {
        ringEl.innerHTML = '<p class="muted" style="padding:24px;text-align:center">No quality data</p>';
        barsEl.innerHTML = '';
        return;
    }

    // Compute completeness per column
    const colStats = {};
    let totalCompleteness = 0;

    cols.forEach((col) => {
        let nonNull = 0;
        rows.forEach((row) => {
            if (row[col] !== null && row[col] !== undefined && row[col] !== '') nonNull++;
        });
        const pct = Math.round((nonNull / rows.length) * 100);
        colStats[col] = pct;
        totalCompleteness += pct;
    });

    const overall = Math.round(totalCompleteness / cols.length);

    // Ring
    const color = overall >= 80 ? 'var(--green)' : overall >= 50 ? 'var(--amber)' : 'var(--red)';
    const circum = 2 * Math.PI * 58;
    const offset = circum - (overall / 100) * circum;

    ringEl.innerHTML = `
        <svg width="148" height="148" viewBox="0 0 148 148">
            <circle cx="74" cy="74" r="58" fill="none" stroke="var(--border)" stroke-width="5"/>
            <circle cx="74" cy="74" r="58" fill="none" stroke="${color}" stroke-width="5"
                stroke-dasharray="${circum}" stroke-dashoffset="${offset}"
                stroke-linecap="round" transform="rotate(-90 74 74)"
                style="transition: stroke-dashoffset 1s ease"/>
            <text x="74" y="70" text-anchor="middle" fill="${color}" font-size="28" font-weight="800">${overall}%</text>
            <text x="74" y="90" text-anchor="middle" fill="var(--text-sec)" font-size="10">COMPLETENESS</text>
        </svg>
    `;

    // Bars
    const entries = Object.entries(colStats).sort((a, b) => a[1] - b[1]);
    barsEl.innerHTML = entries.map(([col, pct]) => {
        const cls = pct >= 80 ? '' : pct >= 50 ? ' mid' : ' low';
        return `<div class="q-bar-row">
            <span class="q-bar-label" title="${esc(col)}">${esc(col)}</span>
            <div class="q-bar-track"><div class="q-bar-fill${cls}" style="width:${pct}%"></div></div>
            <span class="q-bar-pct">${pct}%</span>
        </div>`;
    }).join('');
}

// ── Downloads ───────────────────────────────────────────────────
function setupDownloads() {
    $('dl-csv').onclick = () => {
        if (jobId) window.location.href = `/api/download/${jobId}?format=csv`;
    };
    $('dl-xlsx').onclick = () => {
        if (jobId) window.location.href = `/api/download/${jobId}?format=xlsx`;
    };
}

// ── Append Flow ─────────────────────────────────────────────────
function initAppend() {
    const zone = $('append-zone');
    const input = $('append-input');

    zone.addEventListener('click', () => input.click());
    zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('drag-over');
        if (e.dataTransfer.files.length) doAppendUpload(e.dataTransfer.files[0]);
    });
    input.addEventListener('change', () => { if (input.files.length) doAppendUpload(input.files[0]); });
}

async function doAppendUpload(file) {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('job_id', jobId);

    try {
        const resp = await fetch('/api/append/upload', { method: 'POST', body: fd });
        if (!resp.ok) {
            const err = await resp.json();
            showError(err.detail || 'Append upload failed');
            return;
        }

        appendUploadData = await resp.json();
        appendSelectedSheets = new Set();
        appendUploadData.sheets.forEach((s) => appendSelectedSheets.add(s.sheet_name));

        showAppendModal();
    } catch (err) {
        showError(`Upload failed: ${err.message}`);
    }
}

function showAppendModal() {
    const modal = $('append-modal');
    modal.classList.remove('hidden');

    // Render sheet chips
    const wrap = $('append-sheet-summary');
    _rebuildSheetChips(wrap, appendUploadData.sheets, appendSelectedSheets, true);

    // Build mapping table
    buildAppendMappingTable();
}

function buildAppendMappingTable() {
    const tbody = $('append-mapping-tbody');
    const sheets = (appendUploadData?.sheets || []).filter((s) => appendSelectedSheets.has(s.sheet_name));

    const sampleMap = {};
    const colSet = new Set();
    sheets.forEach((s) => {
        s.columns.forEach((c) => {
            colSet.add(c);
            if (!sampleMap[c] && s.sample_values[c]) sampleMap[c] = s.sample_values[c];
        });
    });

    const allCols = [...colSet];
    const existingCols = appendUploadData.existing_columns || [];
    const targetOptions = ['account_id', 'close_date', ...existingCols.filter((c) => c !== 'account_id' && c !== 'close_date')];

    // Also add new columns
    allCols.forEach((c) => {
        if (!targetOptions.includes(c)) targetOptions.push(c);
    });

    const optionsHtml = `<option value="">— skip —</option>` +
        targetOptions.map((f) => {
            const isKey = f === 'account_id' || f === 'close_date';
            const isExisting = existingCols.includes(f);
            const label = isKey ? f + ' ★' : isExisting ? f + ' (existing)' : f + ' (new)';
            return `<option value="${esc(f)}">${esc(label)}</option>`;
        }).join('');

    let rows = '';
    for (const rawCol of allCols) {
        const samples = (sampleMap[rawCol] || []).slice(0, 6);
        let samplesHtml = '<span class="muted">—</span>';
        if (samples.length) {
            samplesHtml = `<div class="samples-wrap">${samples.map((s) =>
                `<span class="sample-pill">${esc(trunc(String(s), 36))}</span>`
            ).join('')}</div>`;
        }

        rows += `<tr>
            <td class="col-name">${esc(rawCol)}</td>
            <td>${samplesHtml}</td>
            <td><select class="mapping-select append-mapping-select" data-raw="${esc(rawCol)}">${optionsHtml}</select></td>
        </tr>`;
    }

    tbody.innerHTML = rows;

    // Auto-detect mappings
    document.querySelectorAll('.append-mapping-select').forEach((sel) => {
        const raw = sel.dataset.raw.toLowerCase().trim();
        if (raw.includes('account') && raw.includes('id') || raw === 'accountid' || raw === 'account_id') {
            sel.value = 'account_id';
        } else if (raw.includes('close') && raw.includes('date') || raw === 'closedate' || raw === 'close_date') {
            sel.value = 'close_date';
        } else {
            // Try exact match
            const match = targetOptions.find((t) => t === sel.dataset.raw);
            if (match) sel.value = match;
        }
    });
}

async function doAppendConfirm() {
    const mappings = {};
    document.querySelectorAll('.append-mapping-select').forEach((sel) => {
        mappings[sel.dataset.raw] = sel.value || null;
    });

    // Check required key columns
    const mapped = new Set(Object.values(mappings).filter(Boolean));
    if (!mapped.has('account_id') || !mapped.has('close_date')) {
        showError('Please map columns to account_id and close_date before appending.');
        return;
    }

    hideAppendModal();

    showSection('s-loading');
    const status = $('loading-status');
    const bar = $('loading-progress');
    status.textContent = 'Appending data…';
    bar.style.width = '30%';
    bar.style.background = '';

    try {
        bar.style.width = '60%';
        const resp = await fetch('/api/append/confirm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: jobId,
                selected_sheets: [...appendSelectedSheets],
                mappings: mappings,
            }),
        });

        bar.style.width = '90%';

        if (!resp.ok) {
            const err = await resp.json();
            showSection('s-master');
            if (resp.status === 409) {
                showError(err.detail);
            } else {
                showError(err.detail || 'Append failed');
            }
            return;
        }

        masterData = await resp.json();
        bar.style.width = '100%';

        setTimeout(() => {
            showSection('s-master');
            renderMaster();
        }, 400);
    } catch (err) {
        showSection('s-master');
        showError(`Append failed: ${err.message}`);
    }
}

function hideAppendModal() {
    $('append-modal').classList.add('hidden');
    appendUploadData = null;
    const input = $('append-input');
    if (input) input.value = '';
}

// ── Error Modal ─────────────────────────────────────────────────
function showError(message) {
    $('error-detail').textContent = message;
    $('error-modal').classList.remove('hidden');
}

function hideError() {
    $('error-modal').classList.add('hidden');
}

// ── Tabs ────────────────────────────────────────────────────────
function initTabs() {
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('tab')) {
            const tab = e.target.dataset.tab;
            document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.dataset.tab === tab));
            document.querySelectorAll('.panel').forEach((p) => {
                p.classList.toggle('active', p.id === `p-${tab}`);
            });
        }
    });
}

// ── Reset ───────────────────────────────────────────────────────
function resetAll() {
    uploadData = null;
    masterData = null;
    appendUploadData = null;
    selectedSheets = new Set();
    appendSelectedSheets = new Set();
    jobId = null;
    setStep(1);
    showSection('s-upload');
    const input = $('file-input');
    if (input) input.value = '';
}

// ── Init ────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    initUpload();
    initAppend();
    initTabs();

    $('merge-btn').addEventListener('click', doMerge);
    $('reset-mappings-btn').addEventListener('click', () => {
        document.querySelectorAll('#mapping-tbody .mapping-select').forEach((s) => { s.value = ''; });
        updateMappingCounts();
    });
    $('reset-btn').addEventListener('click', resetAll);

    // Append modal
    $('append-modal-close').addEventListener('click', hideAppendModal);
    $('append-cancel-btn').addEventListener('click', hideAppendModal);
    $('append-confirm-btn').addEventListener('click', doAppendConfirm);

    // Error modal
    $('error-modal-close').addEventListener('click', hideError);
    $('error-ok-btn').addEventListener('click', hideError);
});
