/**
 * app.js — DataOrgModel Frontend
 *
 * Two-phase processing:
 *   1. Upload → /api/analyze → Mapping Review
 *   2. Confirm → /api/confirm → Results (with quality, anomalies, timeline)
 *
 * Plus: template save/load, interactive timeline canvas
 */

// ── DOM refs ─────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

const dropZone = $('drop-zone');
const fileInput = $('file-input');
const vendorSlider = $('vendor-threshold');
const headerSlider = $('header-threshold');
const vendorVal = $('vendor-val');
const headerVal = $('header-val');

const uploadSection = $('upload-section');
const analyzingSection = $('analyzing-section');
const mappingSection = $('mapping-section');
const processSection = $('processing-section');
const resultsSection = $('results-section');

const analyzeStatus = $('analyze-status');
const analyzeProgress = $('analyze-progress');
const processStatus = $('process-status');
const progressFill = $('progress-fill');

const mappingTbody = $('mapping-tbody');
const matchedCount = $('matched-count');
const llmCount = $('llm-count');
const unmappedCount = $('unmapped-count');
const confirmBtn = $('confirm-mappings-btn');
const resetMappingsBtn = $('reset-mappings-btn');
const saveTemplateBtn = $('save-template-btn');

const auditGrid = $('audit-grid');
const vendorClusters = $('vendor-clusters');
const unmappedList = $('unmapped-list');
const unmappedWrapper = $('unmapped-wrapper');
const previewCount = $('preview-count');
const previewThead = $('preview-thead');
const previewTbody = $('preview-tbody');
const downloadXlsxBtn = $('download-xlsx-btn');
const downloadCsvBtn = $('download-csv-btn');
const resetBtn = $('reset-btn');

const templateBar = $('template-bar');
const templateChips = $('template-chips');

let currentResult = null;
let analyzeData = null;
let originalMappings = null;

// ── Slider updates ───────────────────────────────────────────────
vendorSlider.addEventListener('input', () => vendorVal.textContent = vendorSlider.value + '%');
headerSlider.addEventListener('input', () => headerVal.textContent = headerSlider.value + '%');

// ── Templates — load on page init ────────────────────────────────
async function loadTemplates() {
    try {
        const resp = await fetch('/api/templates');
        const data = await resp.json();
        if (data.templates && data.templates.length > 0) {
            templateBar.classList.remove('hidden');
            templateChips.innerHTML = '';
            data.templates.forEach(t => {
                const chip = document.createElement('button');
                chip.className = 'template-chip';
                chip.innerHTML = `<span>${esc(t.name)}</span><small>${t.column_count} cols</small>`;
                chip.addEventListener('click', () => applyTemplate(t.id));
                templateChips.appendChild(chip);
            });
        } else {
            templateBar.classList.add('hidden');
        }
    } catch (e) {
        /* ignore */
    }
}

async function applyTemplate(templateId) {
    if (!analyzeData) return;
    try {
        const resp = await fetch(`/api/templates/${templateId}`);
        const template = await resp.json();
        const savedMappings = template.mappings || {};

        // Apply template mappings to current analyze data
        analyzeData.mappings.forEach(m => {
            if (savedMappings[m.original]) {
                m.canonical = savedMappings[m.original];
                m.confidence = 'high';
                m.source = 'template';
            }
        });

        renderMappingReview(analyzeData);
    } catch (e) {
        /* ignore */
    }
}

saveTemplateBtn.addEventListener('click', async () => {
    const name = prompt('Template name:');
    if (!name) return;

    const selects = mappingTbody.querySelectorAll('.mapping-select');
    const mappings = {};
    selects.forEach(sel => {
        if (sel.value) mappings[sel.dataset.original] = sel.value;
    });

    try {
        await fetch('/api/templates/save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, mappings }),
        });
        loadTemplates();
    } catch (e) {
        /* ignore */
    }
});

loadTemplates();

// ── Drag & drop ──────────────────────────────────────────────────
dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('dragover'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
});
fileInput.addEventListener('change', () => { if (fileInput.files.length) handleFile(fileInput.files[0]); });

// ── Phase 1: Analyze ─────────────────────────────────────────────
async function handleFile(file) {
    const ext = file.name.split('.').pop().toLowerCase();
    if (!['xlsx', 'csv'].includes(ext)) { alert('Please upload .xlsx or .csv'); return; }

    showSection('analyzing');
    setAnalyzeProgress(0);
    setAnalyzeStatus('Uploading file…');

    const formData = new FormData();
    formData.append('file', file);
    const params = new URLSearchParams({ header_threshold: headerSlider.value });

    try {
        setAnalyzeProgress(15);
        const resp = await fetch(`/api/analyze?${params}`, { method: 'POST', body: formData });
        setAnalyzeProgress(60);
        setAnalyzeStatus('Analyzing columns…');

        if (!resp.ok) { const err = await resp.json(); throw new Error(err.detail || 'Failed'); }

        setAnalyzeProgress(90);
        setAnalyzeStatus('Building review…');

        analyzeData = await resp.json();
        originalMappings = JSON.parse(JSON.stringify(analyzeData.mappings));

        setAnalyzeProgress(100);
        setTimeout(() => renderMappingReview(analyzeData), 350);

    } catch (err) {
        alert('Error: ' + err.message);
        showSection('upload');
    }
}

// ── Mapping Review ───────────────────────────────────────────────
function renderMappingReview(data) {
    showSection('mapping');

    const mappings = data.mappings;
    const categories = data.categories || [];

    let matched = 0, llm = 0, unmatched = 0;
    mappings.forEach(m => {
        if (m.source === 'llm') llm++;
        else if (m.canonical && m.source !== 'unmapped') matched++;
        else unmatched++;
    });
    matchedCount.textContent = `${matched} matched`;
    llmCount.textContent = `${llm} by AI`;
    unmappedCount.textContent = `${unmatched} unmapped`;

    mappingTbody.innerHTML = '';

    const sorted = [...mappings].sort((a, b) => {
        const order = { unmapped: 0, llm: 1, passthrough: 2, template: 2, fuzzy: 3, exact: 4 };
        return (order[a.source] ?? 3) - (order[b.source] ?? 3);
    });

    sorted.forEach(m => {
        const tr = document.createElement('tr');
        tr.dataset.original = m.original;

        let confClass = 'conf-none';
        if (m.confidence === 'high') confClass = 'conf-high';
        else if (m.confidence === 'medium') confClass = 'conf-medium';
        tr.className = confClass;

        // Col: Original name
        const tdOrig = document.createElement('td');
        tdOrig.className = 'col-original';
        tdOrig.innerHTML = `<span class="col-name">${esc(m.original)}</span>`;
        tr.appendChild(tdOrig);

        // Col: Sample values
        const tdSamples = document.createElement('td');
        tdSamples.className = 'col-samples';
        const samples = (m.samples || []).slice(0, 3);
        tdSamples.innerHTML = samples.length
            ? samples.map(s => `<span class="sample-pill">${esc(trunc(String(s), 22))}</span>`).join('')
            : '<span class="no-samples">—</span>';
        tr.appendChild(tdSamples);

        // Col: Dropdown
        const tdMapped = document.createElement('td');
        tdMapped.className = 'col-mapped';
        const select = document.createElement('select');
        select.className = 'mapping-select';
        select.dataset.original = m.original;

        const opts = [
            { value: '', text: '— Not mapped —' },
            { value: m.original, text: `Keep "${trunc(m.original, 28)}"` },
            { value: '---', text: '─────────────', disabled: true },
        ];
        categories.forEach(c => opts.push({ value: c, text: c }));
        opts.forEach(o => {
            const opt = document.createElement('option');
            opt.value = o.value || '';
            opt.textContent = o.text;
            if (o.disabled) opt.disabled = true;
            select.appendChild(opt);
        });

        select.value = m.canonical || m.original;
        select.addEventListener('change', updateMappingStats);
        tdMapped.appendChild(select);
        tr.appendChild(tdMapped);

        // Col: Confidence
        const tdConf = document.createElement('td');
        tdConf.className = 'col-confidence';
        const confLabel = m.confidence === 'none' ? 'unmatched' : m.confidence;
        tdConf.innerHTML = `<span class="conf-badge ${confClass}">${confLabel}</span>`;
        tr.appendChild(tdConf);

        // Col: Source
        const tdSource = document.createElement('td');
        tdSource.className = 'col-source';
        const sourceLabels = { exact: 'Exact', fuzzy: 'Fuzzy', llm: 'AI', unmapped: '—', passthrough: 'Auto', template: 'Template' };
        tdSource.innerHTML = `<span class="source-label source-${m.source}">${sourceLabels[m.source] || m.source}</span>`;
        tr.appendChild(tdSource);

        mappingTbody.appendChild(tr);
    });
}

function updateMappingStats() {
    const selects = mappingTbody.querySelectorAll('.mapping-select');
    let matched = 0, unmatched = 0;
    selects.forEach(sel => {
        if (sel.value && sel.value !== sel.dataset.original) matched++;
        else if (!sel.value) unmatched++;
    });
    matchedCount.textContent = `${matched} matched`;
    unmappedCount.textContent = `${unmatched} unmapped`;
}

// ── Confirm & process ────────────────────────────────────────────
confirmBtn.addEventListener('click', async () => {
    if (!analyzeData) return;
    showSection('processing');
    setProgress(0);
    setStatus('Preparing mappings…');

    const selects = mappingTbody.querySelectorAll('.mapping-select');
    const mappings = {};
    selects.forEach(sel => { mappings[sel.dataset.original] = sel.value || null; });

    try {
        setProgress(20);
        setStatus('Running pipeline…');
        const resp = await fetch('/api/confirm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: analyzeData.job_id,
                mappings,
                vendor_threshold: parseInt(vendorSlider.value),
                header_threshold: parseInt(headerSlider.value),
            }),
        });

        setProgress(70);
        setStatus('Building results…');
        if (!resp.ok) { const err = await resp.json(); throw new Error(err.detail || 'Failed'); }

        setProgress(90);
        const data = await resp.json();
        currentResult = data;

        setProgress(100);
        setStatus('Done!');
        setTimeout(() => renderResults(data), 400);

    } catch (err) {
        alert('Error: ' + err.message);
        showSection('mapping');
    }
});

resetMappingsBtn.addEventListener('click', () => {
    if (originalMappings && analyzeData) {
        analyzeData.mappings = JSON.parse(JSON.stringify(originalMappings));
        renderMappingReview(analyzeData);
    }
});

// ── Section switching ────────────────────────────────────────────
function showSection(which) {
    uploadSection.classList.toggle('hidden', which !== 'upload');
    analyzingSection.classList.toggle('hidden', which !== 'analyzing');
    mappingSection.classList.toggle('hidden', which !== 'mapping');
    processSection.classList.toggle('hidden', which !== 'processing');
    resultsSection.classList.toggle('hidden', which !== 'results');
}

// ── Tab navigation ───────────────────────────────────────────────
document.querySelectorAll('.result-tabs .tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.result-tabs .tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        tab.classList.add('active');
        $('panel-' + tab.dataset.tab).classList.add('active');

        if (tab.dataset.tab === 'timeline' && currentResult) {
            requestAnimationFrame(() => drawTimeline(currentResult.timeline));
        }
    });
});

// ── Render results ───────────────────────────────────────────────
function renderResults(data) {
    showSection('results');
    const a = data.audit;

    // Audit cards
    auditGrid.innerHTML = '';
    const cards = [
        { value: a.total_rows_ingested, label: 'Rows Ingested', icon: '📥', cls: 'accent' },
        { value: a.tabs_processed, label: 'Tabs Processed', icon: '📑', cls: 'purple' },
        { value: a.final_rows, label: 'Final Rows', icon: '✅', cls: 'success' },
        { value: a.unique_vendors, label: 'Unique Vendors', icon: '🏢', cls: 'pink' },
        { value: a.quote_lines_merged, label: 'Lines Merged', icon: '🔗', cls: 'warning' },
        { value: a.duplicates_removed, label: 'Dupes Removed', icon: '🗑', cls: 'accent' },
        { value: a.anomalies_flagged || 0, label: 'Anomalies', icon: '⚠️', cls: 'danger' },
    ];
    cards.forEach((c, i) => {
        const el = document.createElement('div');
        el.className = `audit-card ${c.cls}`;
        el.style.animationDelay = `${i * 0.05}s`;
        el.innerHTML = `
            <div class="card-icon">${c.icon}</div>
            <div class="value">${c.value.toLocaleString()}</div>
            <div class="label">${c.label}</div>`;
        auditGrid.appendChild(el);
    });

    // Data preview
    previewCount.textContent = `${data.preview.length} of ${a.final_rows}`;
    const cols = a.columns.filter(c => c !== '_anomaly_flags');
    renderTable(cols, data.preview);

    // Data quality
    renderQuality(data.quality);

    // Anomalies
    renderAnomalies(data.anomalies);

    // Vendor clusters
    renderClusters(data.vendor_clusters, a);

    // Timeline (defer until tab is viewed)
}

// ── Data Preview Table ───────────────────────────────────────────
function renderTable(columns, rows) {
    previewThead.innerHTML = '<tr>' + columns.map(c => `<th>${esc(c)}</th>`).join('') + '</tr>';
    previewTbody.innerHTML = rows.map(row => {
        const flags = row._anomaly_flags || '';
        const cls = flags ? ' class="flagged-row"' : '';
        return `<tr${cls}>` + columns.map(c => `<td>${esc(String(row[c] ?? ''))}</td>`).join('') + '</tr>';
    }).join('');
}

// ── Data Quality Dashboard ───────────────────────────────────────
function renderQuality(q) {
    if (!q) return;
    const overview = $('quality-overview');
    const bars = $('quality-bars');

    // Score ring
    const scoreColor = q.overall_score >= 80 ? '#22c55e' : q.overall_score >= 50 ? '#f59e0b' : '#ef4444';
    overview.innerHTML = `
        <div class="quality-score-card">
            <div class="score-ring" style="--score:${q.overall_score};--color:${scoreColor}">
                <svg viewBox="0 0 120 120">
                    <circle cx="60" cy="60" r="52" fill="none" stroke="rgba(255,255,255,0.06)" stroke-width="8"/>
                    <circle cx="60" cy="60" r="52" fill="none" stroke="${scoreColor}" stroke-width="8"
                        stroke-dasharray="${q.overall_score * 3.267} 326.7"
                        stroke-linecap="round" transform="rotate(-90 60 60)"
                        class="score-arc"/>
                </svg>
                <div class="score-number">${q.overall_score}%</div>
            </div>
            <div class="score-meta">
                <h3>Overall Quality</h3>
                <p>${q.total_rows.toLocaleString()} rows · ${q.total_columns} columns</p>
            </div>
        </div>
    `;

    // Column bars
    bars.innerHTML = '';
    (q.columns || []).forEach(col => {
        const barColor = col.rating === 'excellent' ? '#22c55e' : col.rating === 'fair' ? '#f59e0b' : '#ef4444';
        const div = document.createElement('div');
        div.className = 'quality-bar-item';
        div.innerHTML = `
            <div class="qb-header">
                <span class="qb-name">${esc(col.name)}</span>
                <span class="qb-pct" style="color:${barColor}">${col.completeness}%</span>
            </div>
            <div class="qb-track">
                <div class="qb-fill" style="width:${col.completeness}%;background:${barColor}"></div>
            </div>
        `;
        bars.appendChild(div);
    });
}

// ── Anomalies ────────────────────────────────────────────────────
function renderAnomalies(anomalies) {
    const container = $('anomaly-content');
    if (!anomalies || anomalies.length === 0) {
        container.innerHTML = `
            <div class="empty-state success-state">
                <div class="empty-icon">✅</div>
                <h3>No Anomalies Detected</h3>
                <p>Your data looks clean — no suspicious patterns found.</p>
            </div>`;
        return;
    }

    container.innerHTML = `
        <div class="anomaly-grid">
            ${anomalies.map(a => {
        const sevClass = a.severity === 'high' ? 'sev-high' : a.severity === 'medium' ? 'sev-medium' : 'sev-low';
        const sevIcon = a.severity === 'high' ? '🔴' : a.severity === 'medium' ? '🟡' : '🔵';
        return `
                    <div class="anomaly-card ${sevClass}">
                        <div class="anomaly-header">
                            <span class="anomaly-icon">${sevIcon}</span>
                            <span class="anomaly-type">${esc(a.type)}</span>
                            <span class="anomaly-count">${a.count}</span>
                        </div>
                        <p class="anomaly-desc">${esc(a.description)}</p>
                    </div>`;
    }).join('')}
        </div>`;
}

// ── Timeline (Canvas Gantt) ──────────────────────────────────────
function drawTimeline(entries) {
    const canvas = $('timeline-canvas');
    const emptyEl = $('timeline-empty');

    if (!entries || entries.length === 0) {
        canvas.classList.add('hidden');
        emptyEl.classList.remove('hidden');
        return;
    }
    canvas.classList.remove('hidden');
    emptyEl.classList.add('hidden');

    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    // Size canvas
    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = Math.max(400, entries.length * 28 + 80) * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = Math.max(400, entries.length * 28 + 80) + 'px';
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = parseInt(canvas.style.height);
    const LEFT = 140;
    const TOP = 40;
    const BAR_H = 18;
    const ROW_H = 26;

    // Compute time range
    let minDate = Infinity, maxDate = -Infinity;
    const parsed = entries.map(e => {
        const s = new Date(e.start);
        const end = e.end ? new Date(e.end) : new Date(s.getTime() + 365 * 86400000);
        if (s.getTime() < minDate) minDate = s.getTime();
        if (end.getTime() > maxDate) maxDate = end.getTime();
        return { ...e, startTs: s.getTime(), endTs: end.getTime() };
    });

    const rangeMs = maxDate - minDate || 1;

    // Background
    ctx.fillStyle = '#0d0d1a';
    ctx.fillRect(0, 0, W, H);

    // Header
    ctx.fillStyle = '#9898b0';
    ctx.font = '11px Inter, sans-serif';
    ctx.textAlign = 'center';

    const yearStart = new Date(minDate).getFullYear();
    const yearEnd = new Date(maxDate).getFullYear();
    for (let y = yearStart; y <= yearEnd; y++) {
        const ts = new Date(y, 0, 1).getTime();
        const x = LEFT + ((ts - minDate) / rangeMs) * (W - LEFT - 20);
        if (x >= LEFT && x <= W - 20) {
            ctx.fillStyle = 'rgba(99,102,241,0.15)';
            ctx.fillRect(x, TOP, 1, H - TOP);
            ctx.fillStyle = '#9898b0';
            ctx.fillText(y.toString(), x, TOP - 8);
        }
    }

    // Color palette
    const colors = ['#6366f1', '#8b5cf6', '#ec4899', '#f59e0b', '#22c55e', '#06b6d4', '#f97316'];

    // Group by vendor
    const vendorMap = {};
    parsed.forEach(e => {
        if (!vendorMap[e.vendor]) vendorMap[e.vendor] = [];
        vendorMap[e.vendor].push(e);
    });

    let row = 0;
    const vendorNames = Object.keys(vendorMap).sort();

    vendorNames.forEach((vendor, vi) => {
        const entries = vendorMap[vendor];
        const color = colors[vi % colors.length];
        const y = TOP + row * ROW_H;

        // Vendor label
        ctx.fillStyle = '#e2e2f0';
        ctx.font = '11px Inter, sans-serif';
        ctx.textAlign = 'right';
        ctx.fillText(trunc(vendor, 16), LEFT - 8, y + BAR_H / 2 + 4);

        // Bars
        entries.forEach(e => {
            const x1 = LEFT + ((e.startTs - minDate) / rangeMs) * (W - LEFT - 20);
            const x2 = LEFT + ((e.endTs - minDate) / rangeMs) * (W - LEFT - 20);
            const bw = Math.max(x2 - x1, 4);

            ctx.fillStyle = color;
            ctx.globalAlpha = 0.8;
            roundRect(ctx, x1, y, bw, BAR_H, 3);
            ctx.fill();
            ctx.globalAlpha = 1;

            // Label inside bar if wide enough
            if (bw > 50) {
                ctx.fillStyle = '#fff';
                ctx.font = '9px Inter, sans-serif';
                ctx.textAlign = 'left';
                const label = e.product || e.id || '';
                ctx.fillText(trunc(label, Math.floor(bw / 6)), x1 + 6, y + 13);
            }
        });

        row += entries.length;
    });
}

function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
}

// ── Vendor Clusters ──────────────────────────────────────────────
function renderClusters(clusters, audit) {
    const container = $('vendor-clusters');
    if (clusters && clusters.length > 0) {
        container.innerHTML = clusters.map(cl => {
            const variants = cl.variants.filter(v => v !== cl.canonical);
            return `
                <div class="cluster-chip">
                    <span class="canonical">${esc(cl.canonical)}</span>
                    <span class="variant-arrow">←</span>
                    <span class="variants">${variants.map(esc).join(', ')}</span>
                </div>`;
        }).join('');
    } else {
        container.innerHTML = '<div class="empty-state"><p>No vendor clusters detected.</p></div>';
    }

    if (audit.unmapped_columns && audit.unmapped_columns.length > 0) {
        unmappedWrapper.classList.remove('hidden');
        unmappedList.innerHTML = audit.unmapped_columns.map(c =>
            `<span class="tag">${esc(c)}</span>`
        ).join('');
    } else {
        unmappedWrapper.classList.add('hidden');
    }
}

// ── Helpers ──────────────────────────────────────────────────────
function setAnalyzeStatus(t) { analyzeStatus.textContent = t; }
function setAnalyzeProgress(p) { analyzeProgress.style.width = p + '%'; }
function setStatus(t) { processStatus.textContent = t; }
function setProgress(p) { progressFill.style.width = p + '%'; }
function trunc(s, n) { return s.length > n ? s.slice(0, n) + '…' : s; }
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

// ── Download ─────────────────────────────────────────────────────
downloadXlsxBtn.addEventListener('click', () => {
    if (!currentResult) return;
    const a = document.createElement('a');
    a.href = `/api/download/${currentResult.job_id}/${currentResult.xlsx_filename}`;
    a.download = currentResult.xlsx_filename;
    a.click();
});
downloadCsvBtn.addEventListener('click', () => {
    if (!currentResult) return;
    const a = document.createElement('a');
    a.href = `/api/download/${currentResult.job_id}/${currentResult.csv_filename}`;
    a.download = currentResult.csv_filename;
    a.click();
});

// ── Reset ────────────────────────────────────────────────────────
resetBtn.addEventListener('click', () => {
    currentResult = null;
    analyzeData = null;
    originalMappings = null;
    fileInput.value = '';
    progressFill.style.width = '0%';
    analyzeProgress.style.width = '0%';
    showSection('upload');
    loadTemplates();
});
