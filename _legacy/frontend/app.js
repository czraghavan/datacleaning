/* ═══════════════════════════════════════════════════════════════
   app.js — Pareto Contract Data Intelligence
   Clean 4-step pipeline: Upload → Map → Transform → Results
   ═══════════════════════════════════════════════════════════════ */

'use strict';

// ── State ───────────────────────────────────────────────────────
let analyzeData = null;   // Phase 1 response
let currentResult = null; // Phase 2 response
let selectedSheets = new Set(); // Sheets selected for merge

// ── DOM refs ────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const sections = ['s-upload', 's-analyzing', 's-mapping', 's-processing', 's-results'];

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

// ── Templates ───────────────────────────────────────────────────
async function loadTemplates() {
    try {
        const resp = await fetch('/api/templates');
        const data = await resp.json();
        const bar = $('template-bar');
        const chips = $('template-chips');
        if (data.templates && data.templates.length > 0) {
            bar.classList.remove('hidden');
            chips.innerHTML = data.templates.map((t) =>
                `<button class="btn btn-sm btn-secondary" onclick="applyTemplate('${t.id}')">${esc(t.name)}</button>`
            ).join('');
        }
    } catch (e) { /* quiet */ }
}

async function applyTemplate(id) {
    if (!analyzeData) return;
    try {
        const resp = await fetch(`/api/templates/${id}`);
        if (!resp.ok) return;
        const tmpl = await resp.json();
        if (tmpl.mappings) {
            document.querySelectorAll('.mapping-select').forEach((sel) => {
                const raw = sel.dataset.raw;
                const canonical = tmpl.mappings[raw];
                if (canonical) sel.value = canonical;
            });
            updateMappingCounts();
        }
    } catch (e) { /* quiet */ }
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
        if (e.dataTransfer.files.length) uploadFile(e.dataTransfer.files[0]);
    });
    input.addEventListener('change', () => { if (input.files.length) uploadFile(input.files[0]); });

    // Threshold slider
    const slider = $('header-threshold');
    const valSpan = $('header-val');
    slider.addEventListener('input', () => { valSpan.textContent = slider.value + '%'; });
}

// ── Phase 1: Analyze ────────────────────────────────────────────
async function uploadFile(file) {
    showSection('s-analyzing');
    setStep(1);

    const status = $('analyze-status');
    const bar = $('analyze-progress');

    status.textContent = `Analyzing ${file.name}…`;
    bar.style.width = '20%';

    const fd = new FormData();
    fd.append('file', file);
    fd.append('header_threshold', $('header-threshold').value);

    try {
        bar.style.width = '50%';
        const resp = await fetch('/api/analyze', { method: 'POST', body: fd });
        bar.style.width = '90%';
        analyzeData = await resp.json();

        if (analyzeData.error) {
            status.textContent = `Error: ${analyzeData.error}`;
            bar.style.width = '100%';
            bar.style.background = 'var(--red)';
            return;
        }

        bar.style.width = '100%';

        setTimeout(() => {
            setStep(2);
            showSection('s-mapping');
            renderSheetSummary();
            buildMappingTable();
            loadTemplates();
        }, 400);
    } catch (err) {
        status.textContent = `Failed: ${err.message}`;
    }
}

// ── Sheet summary chips ─────────────────────────────────────────
function renderSheetSummary() {
    const wrap = $('sheet-summary');

    // Initialize selectedSheets with all profiled sheets
    selectedSheets = new Set();
    if (analyzeData.profiling) {
        analyzeData.profiling.forEach((p) => selectedSheets.add(p.sheet_name));
    }

    _rebuildSheetChips(wrap);
    _initSheetChipListeners();
}

function _rebuildSheetChips(wrap) {
    if (!wrap) wrap = $('sheet-summary');
    let html = '';

    // Select / Deselect controls
    const totalSheets = (analyzeData.profiling || []).length;
    if (totalSheets > 1) {
        html += `<div class="sheet-select-actions">
            <span class="sheet-select-label">Select sheets to merge:</span>
            <button class="sheet-select-link" id="btn-select-all">Select All</button>
            <span class="sheet-select-sep">·</span>
            <button class="sheet-select-link" id="btn-deselect-all">Deselect All</button>
        </div>`;
    }

    html += '<div class="sheet-chips-row">';

    // Processed sheets (selectable)
    if (analyzeData.profiling) {
        analyzeData.profiling.forEach((p) => {
            const isSelected = selectedSheets.has(p.sheet_name);
            const cls = isSelected ? 'selected' : 'deselected';
            const check = isSelected ? '✓' : '';
            html += `<div class="sheet-chip ${cls}" data-sheet="${esc(p.sheet_name)}" title="Click to ${isSelected ? 'exclude' : 'include'}">
                <span class="sheet-check">${check}</span>
                ${esc(p.sheet_name)}
                <span class="muted">${fmt(p.row_count)} rows · ${fmt(p.column_count)} cols</span>
            </div>`;
        });
    }

    // Skipped sheets (not selectable)
    if (analyzeData.sheets_skipped) {
        analyzeData.sheets_skipped.forEach((s) => {
            html += `<div class="sheet-chip skipped" title="${esc(s.reason)}">
                <span class="dot"></span>
                ${esc(s.name)}
                <span class="muted">skipped</span>
            </div>`;
        });
    }

    html += '</div>';
    wrap.innerHTML = html;
    _initSheetChipListeners();
}

function _initSheetChipListeners() {
    // Use event delegation for sheet chips (handles special chars in names)
    document.querySelectorAll('.sheet-chip[data-sheet]').forEach((chip) => {
        chip.onclick = () => {
            const name = chip.dataset.sheet;
            if (selectedSheets.has(name)) {
                selectedSheets.delete(name);
            } else {
                selectedSheets.add(name);
            }
            _rebuildSheetChips();
            buildMappingTable();
        };
    });

    const selAllBtn = $('btn-select-all');
    const deselAllBtn = $('btn-deselect-all');
    if (selAllBtn) {
        selAllBtn.onclick = () => {
            if (analyzeData.profiling) {
                analyzeData.profiling.forEach((p) => selectedSheets.add(p.sheet_name));
            }
            _rebuildSheetChips();
            buildMappingTable();
        };
    }
    if (deselAllBtn) {
        deselAllBtn.onclick = () => {
            selectedSheets.clear();
            _rebuildSheetChips();
            buildMappingTable();
        };
    }
}

// ── Mapping table ───────────────────────────────────────────────
function buildMappingTable() {
    const tbody = $('mapping-tbody');
    const proposed = analyzeData.proposed_mappings || {};
    const unmapped = analyzeData.unmapped_columns || [];
    const confidence = analyzeData.confidence || {};
    const canonicalFields = analyzeData.canonical_fields || [];

    // Filter profiling to only selected sheets
    const profiling = (analyzeData.profiling || []).filter(
        (p) => selectedSheets.has(p.sheet_name)
    );

    // Build sample map from SELECTED sheets only
    const sampleMap = {};
    const selectedCols = new Set();
    profiling.forEach((sheet) => {
        Object.entries(sheet.columns || {}).forEach(([col, info]) => {
            selectedCols.add(col);
            if (!sampleMap[col]) sampleMap[col] = info.top_values || [];
        });
    });

    // Build all raw columns — only show columns from selected sheets
    const allCols = [
        ...Object.keys(proposed).filter((c) => selectedCols.has(c)),
        ...unmapped.filter((c) => !(c in proposed) && selectedCols.has(c)),
    ];

    // Build dropdown options
    const optionsHtml = `<option value="">— skip —</option>` +
        canonicalFields.map((f) =>
            `<option value="${esc(f.name)}">${esc(f.name)}${f.required ? ' ★' : ''}</option>`
        ).join('');

    let rows = '';
    if (allCols.length === 0) {
        rows = `<tr><td colspan="4" style="text-align:center;padding:24px;color:var(--text-sec)">No sheets selected — select at least one sheet above to see columns</td></tr>`;
    }
    for (const rawCol of allCols) {
        const mapped = proposed[rawCol] || '';
        const conf = confidence[rawCol] || {};
        const score = conf.score || 0;
        const samples = (sampleMap[rawCol] || []).slice(0, 30);

        // Confidence badge
        let confHtml = '';
        if (mapped && score > 0) {
            const cls = score >= 90 ? 'conf-high' : score >= 70 ? 'conf-medium' : 'conf-low';
            confHtml = `<span class="conf-badge ${cls}">${score}%</span>`;
        }

        // Samples display
        let samplesHtml = '<span class="muted">—</span>';
        if (samples.length) {
            samplesHtml = `<div class="samples-wrap">${samples.map((s) => `<span class="sample-pill">${esc(trunc(String(s), 36))}</span>`).join('')
                }</div>`;
        }

        rows += `<tr>
            <td class="col-name">${esc(rawCol)}</td>
            <td>${samplesHtml}</td>
            <td><select class="mapping-select" data-raw="${esc(rawCol)}">${optionsHtml}</select></td>
            <td>${confHtml}</td>
        </tr>`;
    }

    tbody.innerHTML = rows;

    // Set selected values
    document.querySelectorAll('.mapping-select').forEach((sel) => {
        const raw = sel.dataset.raw;
        if (proposed[raw]) sel.value = proposed[raw];
        sel.addEventListener('change', updateMappingCounts);
    });

    updateMappingCounts();

    // Populate primary sheet dropdown
    const primarySelect = $('primary-sheet-select');
    if (primarySelect) {
        primarySelect.innerHTML = '<option value="">Auto-detect (Largest)</option>';
        [...selectedSheets].forEach(sheet => {
            const opt = document.createElement('option');
            opt.value = sheet;
            opt.textContent = sheet;
            primarySelect.appendChild(opt);
        });
    }
}

function updateMappingCounts() {
    const selects = document.querySelectorAll('.mapping-select');
    let mapped = 0, skipped = 0;
    selects.forEach((s) => { if (s.value) mapped++; else skipped++; });
    $('matched-count').textContent = `${mapped} mapped`;
    $('unmapped-count').textContent = `${skipped} skipped`;
}

// ── Phase 2: Confirm ────────────────────────────────────────────
async function confirmMappings() {
    showSection('s-processing');
    setStep(3);

    const status = $('process-status');
    const bar = $('progress-fill');
    status.textContent = 'Transforming data…';
    bar.style.width = '30%';

    // Gather mappings
    const mappings = {};
    document.querySelectorAll('.mapping-select').forEach((sel) => {
        mappings[sel.dataset.raw] = sel.value || null;
    });

    // Get primary sheet
    const primarySelect = $('primary-sheet-select');
    const primarySheetId = primarySelect ? primarySelect.value : null;

    try {
        bar.style.width = '60%';
        const resp = await fetch('/api/confirm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: analyzeData.job_id,
                mappings: mappings,
                selected_sheets: [...selectedSheets],
                primary_sheet_id: primarySheetId || null,
            }),
        });
        bar.style.width = '90%';
        currentResult = await resp.json();

        if (currentResult.error) {
            status.textContent = `Error: ${currentResult.error}`;
            bar.style.background = 'var(--red)';
            return;
        }

        bar.style.width = '100%';
        setTimeout(() => {
            setStep(4);
            showSection('s-results');
            renderResults();
        }, 500);
    } catch (err) {
        status.textContent = `Failed: ${err.message}`;
    }
}

// ── Results ─────────────────────────────────────────────────────
function renderResults() {
    const data = currentResult;
    const val = data.validation || {};
    const passed = val.passed !== false;

    // Banner
    $('banner-icon').textContent = passed ? '✅' : '⚠️';
    $('banner-title').textContent = passed ? 'Pipeline Complete' : 'Completed with Warnings';
    $('banner-sub').textContent = `${fmt(data.rows_after)} contracts · ${(data.columns || []).length} fields`;

    // KPIs
    const kpis = [
        { val: fmt(data.rows_before), label: 'Rows Ingested' },
        { val: fmt(data.rows_after), label: 'Contracts' },
        { val: (data.columns || []).length, label: 'Fields' },
        { val: data.ml_ready ? 'Ready' : 'Not Ready', label: 'ML Status' },
    ];
    $('kpi-grid').innerHTML = kpis.map((k) =>
        `<div class="kpi"><div class="kpi-val">${k.val}</div><div class="kpi-label">${k.label}</div></div>`
    ).join('');

    renderDataPreview(data);
    renderAuditTrail(data);
    renderQuality(data.quality);
    renderValidation(val);
    setupDownloads(data);
}

// ── Data Preview ────────────────────────────────────────────────
function renderDataPreview(data) {
    const cols = data.columns || [];
    const rows = data.preview || [];
    const total = data.rows_after != null ? Number(data.rows_after) : rows.length;

    $('preview-count').textContent = total === 0
        ? 'No rows in output'
        : `Showing ${rows.length} of ${fmt(total)} rows`;

    if (cols.length === 0) {
        $('data-thead').innerHTML = '<tr><th>—</th></tr>';
        $('data-tbody').innerHTML = '<tr><td class="muted">No columns</td></tr>';
        return;
    }
    $('data-thead').innerHTML = `<tr>${cols.map((c) => `<th>${esc(c)}</th>`).join('')}</tr>`;
    $('data-tbody').innerHTML = rows.length === 0
        ? `<tr><td colspan="${cols.length}" class="muted" style="text-align:center;padding:24px">No preview rows</td></tr>`
        : rows.map((row) =>
            `<tr>${cols.map((c) => `<td title="${esc(fmt(row[c]))}">${esc(trunc(fmt(row[c]), 40))}</td>`).join('')}</tr>`
        ).join('');
}

// ── Pipeline Audit ──────────────────────────────────────────────
function renderAuditTrail(data) {
    const trail = $('audit-trail');
    const cls = data.classification || {};
    const val = data.validation || {};
    const versions = data.versions || {};

    const layers = [
        { name: 'Ingestion', detail: `${fmt(data.rows_before)} rows ingested from ${analyzeData?.sheets_processed || '?'} sheets` },
        { name: 'Profiling', detail: `${(analyzeData?.profiling || []).length} sheets profiled` },
        { name: 'Schema Mapping', detail: `${(data.columns || []).length} fields mapped to canonical schema` },
        { name: 'Transformation', detail: `${fmt(data.rows_before)} rows → ${fmt(data.rows_after)} contracts (classification: ${cls.level || '—'})` },
        { name: 'Validation', detail: `${val.passed !== false ? 'Passed' : 'Issues found'} — ${(val.errors || []).length} errors, ${(val.warnings || []).length} warnings`, warn: !val.passed },
        { name: 'Canonical Output', detail: `${(data.columns || []).length} canonical fields produced` },
        { name: 'Versioning', detail: `Schema ${versions.schema_version || 'v1'} · Mapping ${versions.mapping_version || 'v1'}` },
    ];

    trail.innerHTML = layers.map((l) => `
        <div class="audit-layer${l.warn ? ' warn' : ''}">
            <div class="audit-layer-title">${l.name}</div>
            <div class="audit-layer-detail">${l.detail}</div>
        </div>
    `).join('');
}

// ── Quality ─────────────────────────────────────────────────────
function renderQuality(quality) {
    const ringEl = $('quality-ring');
    const barsEl = $('quality-bars');
    if (!ringEl || !barsEl) return;
    if (!quality) {
        ringEl.innerHTML = '<p class="muted" style="padding:24px;text-align:center">No quality data</p>';
        barsEl.innerHTML = '';
        return;
    }
    const score = quality.overall_score ?? 0;
    const cols = quality.columns || {};

    // Ring
    const pct = Math.round(score);
    const color = pct >= 80 ? 'var(--green)' : pct >= 50 ? 'var(--amber)' : 'var(--red)';
    const circum = 2 * Math.PI * 58;
    const offset = circum - (pct / 100) * circum;
    $('quality-ring').innerHTML = `
        <svg width="148" height="148" viewBox="0 0 148 148">
            <circle cx="74" cy="74" r="58" fill="none" stroke="var(--border)" stroke-width="5"/>
            <circle cx="74" cy="74" r="58" fill="none" stroke="${color}" stroke-width="5"
                stroke-dasharray="${circum}" stroke-dashoffset="${offset}"
                stroke-linecap="round" transform="rotate(-90 74 74)"
                style="transition: stroke-dashoffset 1s ease"/>
            <text x="74" y="70" text-anchor="middle" fill="${color}" font-size="28" font-weight="800">${pct}%</text>
            <text x="74" y="90" text-anchor="middle" fill="var(--text-sec)" font-size="10">COMPLETENESS</text>
        </svg>
    `;

    // Bars
    const entries = Object.entries(cols).sort((a, b) => (a[1].completeness || 0) - (b[1].completeness || 0));
    barsEl.innerHTML = entries.length === 0
        ? '<p class="muted" style="padding:16px;text-align:center;font-size:0.9rem">No column completeness data</p>'
        : entries.map(([col, info]) => {
        const p = info.completeness;
        const cls = p >= 80 ? '' : p >= 50 ? ' mid' : ' low';
        return `<div class="q-bar-row">
            <span class="q-bar-label" title="${esc(col)}">${esc(col)}</span>
            <div class="q-bar-track"><div class="q-bar-fill${cls}" style="width:${p}%"></div></div>
            <span class="q-bar-pct">${p}%</span>
        </div>`;
    }).join('');
}

// ── Validation ──────────────────────────────────────────────────
function renderValidation(val) {
    const container = $('val-content');
    if (!container) return;
    if (!val) {
        container.innerHTML = '<div class="muted" style="padding:24px;text-align:center">No validation data</div>';
        return;
    }
    const errors = val.errors || [];
    const warnings = val.warnings || [];
    const info = val.info || [];

    let html = `<div class="val-summary">
        <div class="val-card"><div class="val-card-num" style="color:var(--red)">${errors.length}</div><div class="val-card-label">Errors</div></div>
        <div class="val-card"><div class="val-card-num" style="color:var(--amber)">${warnings.length}</div><div class="val-card-label">Warnings</div></div>
        <div class="val-card"><div class="val-card-num" style="color:var(--indigo)">${info.length}</div><div class="val-card-label">Info</div></div>
    </div>`;

    const allIssues = [
        ...errors.map((e) => ({ ...e, sev: 'error' })),
        ...warnings.map((w) => ({ ...w, sev: 'warning' })),
        ...info.map((i) => ({ ...i, sev: 'info' })),
    ];

    if (allIssues.length > 0) {
        html += `<div class="val-issues">${allIssues.slice(0, 30).map((issue) =>
            `<div class="val-issue">
                <div class="val-sev ${issue.sev}"></div>
                <div>${esc(issue.message || issue.type || issue.rule || JSON.stringify(issue))}</div>
            </div>`
        ).join('')}</div>`;
    } else {
        html += `<div class="val-empty">No validation issues. Output meets configured rules.</div>`;
    }

    container.innerHTML = html;
}

// ── Downloads ───────────────────────────────────────────────────
function setupDownloads(data) {
    const downloads = data.downloads || {};
    const excelUrl = downloads.excel;
    const csvUrl = downloads.csv || downloads.contracts_csv;
    const parquetUrl = downloads.parquet || downloads.contracts_parquet;

    $('dl-xlsx').onclick = () => excelUrl && (window.location.href = excelUrl);
    $('dl-csv').onclick = () => csvUrl && (window.location.href = csvUrl);
    $('dl-parquet').onclick = () => parquetUrl && (window.location.href = parquetUrl);

    // Disable download buttons when file not available
    $('dl-xlsx').disabled = !excelUrl;
    $('dl-csv').disabled = !csvUrl;
    $('dl-parquet').disabled = !parquetUrl;

    // PDF report
    $('dl-pdf').onclick = () => generatePdf(data);
}

// ── PDF Report ──────────────────────────────────────────────────
function generatePdf(data) {
    const w = window.open('', '_blank');
    const val = data.validation || {};
    const q = data.quality || {};
    const cols = data.columns || [];
    const preview = data.preview || [];

    const html = `<!DOCTYPE html>
<html><head><title>Pipeline Report — ${analyzeData?.filename || ''}</title>
<style>
body { font-family: 'Inter', -apple-system, sans-serif; max-width: 800px; margin: 40px auto; color: #1a1a2e; font-size: 13px; line-height: 1.5; }
h1 { font-size: 22px; border-bottom: 2px solid #6366f1; padding-bottom: 8px; margin-bottom: 16px; }
h2 { font-size: 15px; color: #6366f1; margin: 24px 0 8px; }
.kpis { display: flex; gap: 12px; margin: 16px 0; }
.kpi-box { flex:1; padding: 12px; border: 1px solid #e5e7eb; border-radius: 8px; text-align: center; }
.kpi-box .num { font-size: 22px; font-weight: 800; color: #6366f1; }
.kpi-box .lbl { font-size: 10px; color: #888; text-transform: uppercase; }
table { width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 12px; }
th { background: #f8f9fa; text-align: left; padding: 6px 8px; border-bottom: 2px solid #e5e7eb; }
td { padding: 5px 8px; border-bottom: 1px solid #f0f0f0; }
.sev { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.sev-error { background: #ef4444; }
.sev-warning { background: #f59e0b; }
.sev-info { background: #6366f1; }
@media print { body { margin: 20px; } }
</style></head><body>
<h1>Pipeline Audit Report</h1>
<p><strong>File:</strong> ${analyzeData?.filename || '—'} &nbsp; <strong>Date:</strong> ${new Date().toLocaleDateString()}</p>
<div class="kpis">
<div class="kpi-box"><div class="num">${fmt(data.rows_before)}</div><div class="lbl">Rows Ingested</div></div>
<div class="kpi-box"><div class="num">${fmt(data.rows_after)}</div><div class="lbl">Contracts</div></div>
<div class="kpi-box"><div class="num">${cols.length}</div><div class="lbl">Fields</div></div>
<div class="kpi-box"><div class="num">${Math.round(q.overall_score || 0)}%</div><div class="lbl">Quality</div></div>
</div>

<h2>Validation Summary</h2>
<p>${(val.errors || []).length} errors · ${(val.warnings || []).length} warnings · ${(val.info || []).length} info</p>
${(val.errors || []).concat(val.warnings || []).concat(val.info || []).slice(0, 15).map((i, idx) => {
        const sev = idx < (val.errors || []).length ? 'error' : idx < (val.errors || []).length + (val.warnings || []).length ? 'warning' : 'info';
        return `<p><span class="sev sev-${sev}"></span>${i.message || i.rule || JSON.stringify(i)}</p>`;
    }).join('')}

<h2>Data Preview (first ${Math.min(preview.length, 15)} rows)</h2>
<table>
<tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>
${preview.slice(0, 15).map(r => `<tr>${cols.map(c => `<td>${fmt(r[c])}</td>`).join('')}</tr>`).join('')}
</table>
</body></html>`;

    w.document.write(html);
    w.document.close();
    setTimeout(() => w.print(), 600);
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

// ── Save template ───────────────────────────────────────────────
async function saveTemplate() {
    const name = prompt('Template name:');
    if (!name) return;
    const mappings = {};
    document.querySelectorAll('.mapping-select').forEach((sel) => {
        mappings[sel.dataset.raw] = sel.value || null;
    });
    try {
        await fetch('/api/templates', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, mappings }),
        });
    } catch (e) { /* quiet */ }
}

// ── Reset ───────────────────────────────────────────────────────
function resetAll() {
    analyzeData = null;
    currentResult = null;
    selectedSheets = new Set();
    setStep(1);
    showSection('s-upload');
    const input = $('file-input');
    if (input) input.value = '';
}

// ── Init ────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    initUpload();
    initTabs();

    $('confirm-btn').addEventListener('click', confirmMappings);
    $('save-template-btn').addEventListener('click', saveTemplate);
    $('reset-mappings-btn').addEventListener('click', () => {
        document.querySelectorAll('.mapping-select').forEach((s) => { s.value = ''; });
        updateMappingCounts();
    });
    $('reset-btn').addEventListener('click', resetAll);
});
