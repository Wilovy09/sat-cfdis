/* ─── Pulso SAT — analytics.js ──────────────────────────────────────────────
   Fetches all 10 analytics endpoints and renders charts + tables.
   Depends on Chart.js (loaded before this file).
   ─────────────────────────────────────────────────────────────────────────── */
'use strict';

/* ── Chart.js global defaults ──────────────────────────────────────────── */
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif";
Chart.defaults.font.size   = 12;
Chart.defaults.color       = '#6b7280';

/* ── Color palette ─────────────────────────────────────────────────────── */
const C = {
  primary:  '#4f46e5',
  success:  '#10b981',
  warning:  '#f59e0b',
  danger:   '#ef4444',
  indigo:   '#6366f1',
  teal:     '#14b8a6',
  purple:   '#8b5cf6',
  pink:     '#ec4899',
  palette:  ['#4f46e5','#10b981','#f59e0b','#ef4444','#6366f1','#14b8a6','#8b5cf6','#ec4899','#f97316','#06b6d4'],
};

/* ── Format helpers ────────────────────────────────────────────────────── */
const fmtMXN = v =>
  '$' + (v || 0).toLocaleString('es-MX', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
const fmtMXN2 = v =>
  '$' + (v || 0).toLocaleString('es-MX', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtPct  = v => ((v || 0).toFixed(1)) + '%';
const fmtNum  = v => (v || 0).toLocaleString('es-MX');

/* ── Chart registry (destroy before re-create) ─────────────────────────── */
const charts = {};
function mkChart(id, config) {
  if (charts[id]) { charts[id].destroy(); }
  const el = document.getElementById(id);
  if (!el) return;
  charts[id] = new Chart(el, config);
  return charts[id];
}

/* ── Tab switching ─────────────────────────────────────────────────────── */
document.querySelectorAll('#an-tabs .tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#an-tabs .tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.an-tab-panel').forEach(p => p.classList.add('hidden'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab)?.classList.remove('hidden');
  });
});

/* ── Default dates ─────────────────────────────────────────────────────── */
(function setDefaultDates() {
  const now   = new Date();
  const toD   = new Date(now.getFullYear(), now.getMonth() - 1); // last concluded month
  const fromD = new Date(toD.getFullYear() - 1, toD.getMonth() + 1); // 12 months back
  const fmt = d => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
  const fromEl = document.getElementById('an-from');
  const toEl   = document.getElementById('an-to');
  if (fromEl && !fromEl.value) fromEl.value = fmt(fromD);
  if (toEl   && !toEl.value)   toEl.value   = fmt(toD);
})();

/* ── RFC uppercase ─────────────────────────────────────────────────────── */
document.getElementById('an-rfc')?.addEventListener('input', function() {
  const pos = this.selectionStart;
  this.value = this.value.toUpperCase();
  this.setSelectionRange(pos, pos);
});

/* ── Normalization: toggle group-name field ────────────────────────────── */
document.getElementById('norm-action')?.addEventListener('change', function() {
  const wrap = document.getElementById('norm-group-name-wrap');
  if (wrap) wrap.style.display = this.value === 'group' ? '' : 'none';
});

/* ── Payroll norm: toggle pct field ────────────────────────────────────── */
document.getElementById('pnorm-family')?.addEventListener('change', function() {
  const wrap = document.getElementById('pnorm-pct-wrap');
  if (wrap) wrap.style.display = this.value === 'scale_employee_pct' ? '' : 'none';
});

/* ══════════════════════════════════════════════════════════════════════════
   MAIN LOAD
   ══════════════════════════════════════════════════════════════════════════ */

document.getElementById('an-load-btn')?.addEventListener('click', loadAnalytics);
document.getElementById('an-rfc')?.addEventListener('keydown', e => {
  if (e.key === 'Enter') loadAnalytics();
});

async function loadAnalytics() {
  const rfc    = (document.getElementById('an-rfc')?.value || '').trim().toUpperCase();
  const from   = document.getElementById('an-from')?.value || '';
  const to     = document.getElementById('an-to')?.value   || '';
  const dlType = document.getElementById('an-dltype')?.value || 'emitidos';

  if (!rfc) { showAnError('Ingresa un RFC.'); return; }

  setLoading(true);
  clearAnError();
  document.getElementById('an-dashboard')?.classList.add('hidden');
  document.getElementById('an-empty')?.classList.add('hidden');

  const base = `/api/v1/analytics/${encodeURIComponent(rfc)}`;
  const qs   = `?dl_type=${dlType}&from=${from}&to=${to}&limit=100`;
  const qs50 = `?dl_type=${dlType}&from=${from}&to=${to}&limit=50`;

  try {
    const [summary, cp, rec, ret, geo, con, fis, pay, cf, nom] = await Promise.all([
      fetchJSON(`${base}/summary${qs}`),
      fetchJSON(`${base}/counterparties${qs}`),
      fetchJSON(`${base}/recurrence${qs}`),
      fetchJSON(`${base}/retention${qs}`),
      fetchJSON(`${base}/geography${qs}`),
      fetchJSON(`${base}/concepts${qs50}`),
      fetchJSON(`${base}/fiscal${qs}`),
      fetchJSON(`${base}/payments${qs}`),
      fetchJSON(`${base}/cashflow${qs}`),
      fetchJSON(`${base}/payroll${qs}`),
    ]);

    // Show dashboard BEFORE rendering charts — Chart.js needs visible canvas dimensions
    document.getElementById('an-dashboard')?.classList.remove('hidden');

    renderSummary(summary);
    renderCounterparties(cp);
    renderRecurrence(rec);
    renderRetention(ret);
    renderGeography(geo);
    renderConcepts(con);
    renderFiscal(fis);
    renderPayments(pay);
    renderCashflow(cf);
    renderPayroll(nom);

    // Load normalization rules on first open
    loadNormRules(rfc, dlType);
    loadPayrollNormRules(rfc);

    // Wire normalization forms
    wireNormForms(rfc, dlType);
  } catch (err) {
    showAnError(err.message);
    document.getElementById('an-empty')?.classList.remove('hidden');
  } finally {
    setLoading(false);
  }
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || err.message || `HTTP ${res.status}: ${url}`);
  }
  return res.json();
}

function setLoading(on) {
  const btn     = document.getElementById('an-load-btn');
  const label   = document.getElementById('an-load-label');
  const spinner = document.getElementById('an-load-spinner');
  if (btn)     btn.disabled = on;
  if (label)   label.textContent = on ? 'Cargando…' : 'Cargar';
  if (spinner) spinner.classList.toggle('hidden', !on);
}

function showAnError(msg) {
  const el = document.getElementById('an-error');
  if (el) { el.textContent = '⚠ ' + msg; el.classList.remove('hidden'); }
}

function clearAnError() {
  const el = document.getElementById('an-error');
  if (el) { el.textContent = ''; el.classList.add('hidden'); }
}

/* ══════════════════════════════════════════════════════════════════════════
   RESUMEN
   ══════════════════════════════════════════════════════════════════════════ */

function renderSummary(d) {
  setText('kv-total',  fmtMXN(d.total_mxn));
  setText('kv-count',  fmtNum(d.invoice_count));
  setText('kv-avg',    fmtMXN(d.avg_monthly_mxn));
  setText('kv-ltm',    fmtMXN(d.ltm_total_mxn));
  const yoy = d.growth_pct_yoy;
  if (yoy != null) {
    const el = document.getElementById('kv-yoy');
    if (el) {
      el.textContent = (yoy >= 0 ? '+' : '') + yoy.toFixed(1) + '%';
      el.style.color = yoy >= 0 ? 'var(--success)' : 'var(--danger)';
    }
  } else {
    setText('kv-yoy', '—');
  }

  // Monthly bar chart
  const months = d.by_month || [];
  mkChart('chart-monthly', {
    type: 'bar',
    data: {
      labels:   months.map(m => m.period),
      datasets: [{
        label: 'Total MXN',
        data:  months.map(m => m.total_mxn),
        backgroundColor: C.primary + 'cc',
        borderRadius: 4,
      }],
    },
    options: { responsive: true, plugins: { legend: { display: false } },
      scales: { y: { ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Yearly grouped bar
  const years = d.by_year || [];
  mkChart('chart-yearly', {
    type: 'bar',
    data: {
      labels: years.map(y => y.year),
      datasets: [
        { label: 'Ingreso', data: years.map(y => y.ingreso_mxn), backgroundColor: C.success + 'cc', borderRadius: 4 },
        { label: 'Egreso',  data: years.map(y => y.egreso_mxn),  backgroundColor: C.danger  + 'cc', borderRadius: 4 },
      ],
    },
    options: { responsive: true, scales: { y: { ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Tipo table
  fillTable('tbl-tipo', (d.by_tipo || []).map(t => [
    t.label || t.tipo_comprobante,
    fmtMXN(t.total_mxn),
    fmtNum(t.invoice_count),
  ]));
}

/* ══════════════════════════════════════════════════════════════════════════
   CONTRAPARTES
   ══════════════════════════════════════════════════════════════════════════ */

function renderCounterparties(d) {
  setText('kv-cp-count', fmtNum(d.total_unique));
  setText('kv-cp-hhi',   (d.hhi || 0).toFixed(0));
  setText('kv-cp-top10', fmtPct(d.top10_pct));

  const top20 = (d.top || []).slice(0, 20);
  mkChart('chart-cp-top', {
    type: 'bar',
    data: {
      labels:   top20.map(c => truncate(c.name || c.rfc, 28)),
      datasets: [{ label: 'MXN', data: top20.map(c => c.total_mxn),
        backgroundColor: C.palette.map(p => p + 'cc'),
        borderRadius: 3 }],
    },
    options: {
      indexAxis: 'y', responsive: true,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { callback: v => fmtMXN(v) } } },
    },
  });

  fillTable('tbl-cp', (d.top || []).map(c => [
    `<code class="rfc">${c.rfc}</code>`,
    c.name || '—',
    fmtMXN(c.total_mxn),
    fmtNum(c.invoice_count),
    fmtMXN(c.avg_invoice_mxn),
    c.active_months,
    fmtPct(c.pct_of_total),
  ]), true);
}

/* ══════════════════════════════════════════════════════════════════════════
   RECURRENCIA
   ══════════════════════════════════════════════════════════════════════════ */

function renderRecurrence(d) {
  setText('kv-rec-pct', fmtPct(d.recurring_pct));
  setText('kv-rec-one', fmtPct(d.one_time_pct));

  const buckets = d.by_frequency || [];
  mkChart('chart-rec-freq', {
    type: 'doughnut',
    data: {
      labels:   buckets.map(b => b.bucket),
      datasets: [{ data: buckets.map(b => b.counterparty_count),
        backgroundColor: C.palette, borderWidth: 2 }],
    },
    options: { responsive: true, plugins: { legend: { position: 'right' } } },
  });

  fillTable('tbl-rec', (d.top_recurring || []).map(c => [
    `<code class="rfc">${c.rfc}</code>`,
    c.name || '—',
    c.active_months,
    fmtMXN(c.total_mxn),
    fmtPct(c.consistency_pct),
  ]));
}

/* ══════════════════════════════════════════════════════════════════════════
   RETENCIÓN
   ══════════════════════════════════════════════════════════════════════════ */

function renderRetention(d) {
  setText('kv-ret-pct',   fmtPct(d.retention_rate_pct));
  setText('kv-ret-life',  ((d.avg_lifetime_months || 0).toFixed(1)) + 'm');
  setText('kv-ret-churn', fmtNum(d.churned_last3m));
  setText('kv-ret-new',   fmtNum(d.new_last3m));

  renderCohortTable(d.cohorts || []);
}

function renderCohortTable(cohorts) {
  const wrap = document.getElementById('cohort-table-wrap');
  if (!wrap || !cohorts.length) {
    if (wrap) wrap.innerHTML = '<p class="an-empty-msg">Sin datos de cohortes.</p>';
    return;
  }
  // Gather all period columns
  const allPeriods = [];
  cohorts.forEach(c => (c.months || []).forEach(m => {
    if (!allPeriods.includes(m.period)) allPeriods.push(m.period);
  }));
  allPeriods.sort();

  const header = `<tr><th>Cohorte</th><th class="num">Inicial</th>${allPeriods.map(p => `<th class="num">${p}</th>`).join('')}</tr>`;
  const rows = cohorts.slice(0, 24).map(c => {
    const byPeriod = {};
    (c.months || []).forEach(m => { byPeriod[m.period] = m; });
    const cells = allPeriods.map(p => {
      const m = byPeriod[p];
      if (!m) return '<td>—</td>';
      const pct = c.cohort_size > 0 ? m.active / c.cohort_size * 100 : 0;
      const bg  = pct > 60 ? 'var(--success-light)' : pct > 30 ? 'var(--warning-light)' : '';
      return `<td class="num" style="background:${bg}">${m.active} <small>(${pct.toFixed(0)}%)</small></td>`;
    });
    return `<tr><td>${c.cohort}</td><td class="num">${c.cohort_size}</td>${cells.join('')}</tr>`;
  }).join('');

  wrap.innerHTML = `<div class="table-wrap"><table class="invoice-table cohort-table"><thead>${header}</thead><tbody>${rows}</tbody></table></div>`;
}

/* ══════════════════════════════════════════════════════════════════════════
   GEOGRAFÍA
   ══════════════════════════════════════════════════════════════════════════ */

function renderGeography(d) {
  const states = (d.by_state || []).slice(0, 15);
  mkChart('chart-geo-state', {
    type: 'bar',
    data: {
      labels:   states.map(s => s.state_name),
      datasets: [{ label: 'MXN', data: states.map(s => s.total_mxn),
        backgroundColor: C.indigo + 'cc', borderRadius: 4 }],
    },
    options: {
      indexAxis: 'y', responsive: true,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { callback: v => fmtMXN(v) } } },
    },
  });

  fillTable('tbl-geo', (d.by_state || []).map(s => [
    s.state_name,
    fmtMXN(s.total_mxn),
    fmtNum(s.invoice_count),
    fmtPct(s.pct_of_total),
  ]));
}

/* ══════════════════════════════════════════════════════════════════════════
   CONCEPTOS
   ══════════════════════════════════════════════════════════════════════════ */

function renderConcepts(d) {
  const top = (d.top_by_amount || []).slice(0, 15);
  mkChart('chart-concepts', {
    type: 'bar',
    data: {
      labels:   top.map(c => truncate(c.descripcion, 30)),
      datasets: [{ label: 'Importe', data: top.map(c => c.total_importe),
        backgroundColor: C.teal + 'cc', borderRadius: 4 }],
    },
    options: {
      indexAxis: 'y', responsive: true,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { callback: v => fmtMXN(v) } } },
    },
  });

  // Build description map for clave table
  const descMap = {};
  (d.top_by_amount || []).forEach(c => { descMap[c.clave_prod_serv] = c.descripcion; });

  fillTable('tbl-concepts', (d.by_clave || []).map(c => [
    `<code class="rfc">${c.clave_prod_serv}</code>`,
    truncate(descMap[c.clave_prod_serv] || '—', 40),
    fmtMXN(c.total_importe),
    fmtNum(c.invoice_count),
    fmtPct(c.pct_of_total),
  ]), true);
}

/* ══════════════════════════════════════════════════════════════════════════
   FISCAL
   ══════════════════════════════════════════════════════════════════════════ */

function renderFiscal(d) {
  setText('kv-iva-tras', fmtMXN(d.iva_traslado_total));
  setText('kv-iva-ret',  fmtMXN(d.iva_retenido_total));
  setText('kv-iva-neto', fmtMXN((d.iva_traslado_total||0) - (d.iva_retenido_total||0)));
  setText('kv-isr',      fmtMXN(d.isr_retenido_total));
  setText('kv-tax-rate', fmtPct(d.effective_iva_rate));

  // Monthly stacked bar
  const months = d.by_month || [];
  mkChart('chart-fiscal-month', {
    type: 'bar',
    data: {
      labels: months.map(m => m.period),
      datasets: [
        { label: 'IVA traslado', data: months.map(m => m.iva_traslado), backgroundColor: C.success + 'cc', borderRadius: 2 },
        { label: 'IVA retenido', data: months.map(m => m.iva_retenido), backgroundColor: C.danger  + 'cc', borderRadius: 2 },
        { label: 'ISR retenido', data: months.map(m => m.isr_retenido), backgroundColor: C.warning + 'cc', borderRadius: 2 },
      ],
    },
    options: { responsive: true, scales: { x: { stacked: true }, y: { stacked: true, ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Currency table
  fillTable('tbl-currency', (d.by_currency || []).map(c => [
    c.moneda,
    fmtNum(c.invoice_count),
    fmtMXN2(c.total_original),
    fmtMXN(c.total_mxn),
    (c.avg_tipo_cambio || 1).toFixed(4),
    fmtPct(c.pct_of_total),
  ]));

  // Fiscal summary
  const wrap = document.getElementById('fiscal-summary-wrap');
  if (wrap) {
    wrap.innerHTML = `
      <dl class="an-dl">
        <div><dt>IVA 16%</dt><dd>${fmtMXN(d.iva_16_total)}</dd></div>
        <div><dt>IVA 8%</dt><dd>${fmtMXN(d.iva_8_total)}</dd></div>
        <div><dt>IVA exento</dt><dd>${fmtMXN(d.iva_exento_total)}</dd></div>
        <div><dt>IVA cero</dt><dd>${fmtMXN(d.iva_cero_total)}</dd></div>
        <div><dt>IEPS</dt><dd>${fmtMXN(d.ieps_total)}</dd></div>
      </dl>`;
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   PAGOS
   ══════════════════════════════════════════════════════════════════════════ */

function renderPayments(d) {
  setText('kv-pag-inv',  fmtMXN(d.total_invoiced_mxn));
  setText('kv-pag-paid', fmtMXN(d.total_paid_mxn));
  setText('kv-pag-out',  fmtMXN(d.outstanding_mxn));
  setText('kv-pag-rate', fmtPct(d.collection_rate_pct));

  // Forma de pago donut
  const fp = d.by_forma_pago || [];
  mkChart('chart-forma-pago', {
    type: 'doughnut',
    data: {
      labels:   fp.map(f => f.label || f.forma_pago),
      datasets: [{ data: fp.map(f => f.total_mxn),
        backgroundColor: C.palette, borderWidth: 2 }],
    },
    options: { responsive: true, plugins: { legend: { position: 'right' } } },
  });

  // PUE vs PPD bar
  const mp = d.by_metodo_pago || [];
  mkChart('chart-metodo-pago', {
    type: 'bar',
    data: {
      labels:   mp.map(m => m.metodo_pago),
      datasets: [{ label: 'MXN', data: mp.map(m => m.total_mxn),
        backgroundColor: [C.primary + 'cc', C.indigo + 'cc'], borderRadius: 4 }],
    },
    options: { responsive: true, plugins: { legend: { display: false } },
      scales: { y: { ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Outstanding table
  fillTable('tbl-outstanding', (d.outstanding_invoices || []).slice(0, 50).map(inv => [
    `<span class="uuid-cell" title="${inv.uuid}">${(inv.uuid||'').slice(0,13)}…</span>`,
    `<code class="rfc">${inv.counterparty_rfc || '—'}</code>`,
    (inv.fecha_emision || '').slice(0,10),
    fmtMXN(inv.total_mxn),
    fmtMXN(inv.paid_mxn),
    fmtMXN(inv.outstanding_mxn),
  ]), true);
}

/* ══════════════════════════════════════════════════════════════════════════
   CASHFLOW
   ══════════════════════════════════════════════════════════════════════════ */

function renderCashflow(d) {
  setText('kv-cf-pos',     fmtMXN(d.cumulative_position));
  setText('kv-cf-pue',     fmtMXN(d.pue_total_mxn));
  setText('kv-cf-ppd',     fmtMXN(d.ppd_invoiced_mxn));
  setText('kv-cf-ppd-out', fmtMXN(d.ppd_outstanding_mxn));

  // Cumulative line
  const cum = d.by_month || [];
  mkChart('chart-cf-cumulative', {
    type: 'line',
    data: {
      labels:   cum.map(m => m.period),
      datasets: [{
        label: 'Posición acumulada',
        data:  cum.map(m => m.cumulative_mxn),
        borderColor: C.primary, backgroundColor: C.primary + '22',
        fill: true, tension: 0.3, pointRadius: 3,
      }],
    },
    options: { responsive: true, plugins: { legend: { display: false } },
      scales: { y: { ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Ingresos vs egresos
  mkChart('chart-cf-monthly', {
    type: 'bar',
    data: {
      labels: cum.map(m => m.period),
      datasets: [
        { label: 'Ingresos',  data: cum.map(m => m.ingreso_mxn), backgroundColor: C.success + 'cc', borderRadius: 3 },
        { label: 'Egresos',   data: cum.map(m => -(m.egreso_mxn||0)), backgroundColor: C.danger  + 'cc', borderRadius: 3 },
      ],
    },
    options: { responsive: true, scales: { y: { ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Forma de pago (complement payments)
  const fp = d.by_forma_pago || [];
  mkChart('chart-cf-forma', {
    type: 'doughnut',
    data: {
      labels:   fp.map(f => f.label || f.forma_pago),
      datasets: [{ data: fp.map(f => f.monto), backgroundColor: C.palette, borderWidth: 2 }],
    },
    options: { responsive: true, plugins: { legend: { position: 'right' } } },
  });
}

/* ══════════════════════════════════════════════════════════════════════════
   NÓMINA
   ══════════════════════════════════════════════════════════════════════════ */

function renderPayroll(d) {
  setText('kv-nom-total', fmtMXN(d.total_paid_mxn));
  setText('kv-nom-emp',   fmtNum(d.unique_employees));
  setText('kv-nom-avg',   fmtMXN(d.avg_per_employee_mxn));
  setText('kv-nom-sdi',   fmtMXN2(d.avg_sdi));

  // Headcount by month
  const hc = d.by_month || [];
  mkChart('chart-nom-hc', {
    type: 'line',
    data: {
      labels:   hc.map(m => m.period),
      datasets: [{
        label: 'Empleados',
        data:  hc.map(m => m.headcount),
        borderColor: C.purple, backgroundColor: C.purple + '22',
        fill: true, tension: 0.3, pointRadius: 3,
      }],
    },
    options: { responsive: true, plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } },
  });

  // Monthly payroll spend
  mkChart('chart-nom-monthly', {
    type: 'bar',
    data: {
      labels:   hc.map(m => m.period),
      datasets: [
        { label: 'Percepciones', data: hc.map(m => m.total_percepciones), backgroundColor: C.success + 'cc', borderRadius: 3 },
        { label: 'Deducciones',  data: hc.map(m => m.total_deducciones),  backgroundColor: C.danger  + 'cc', borderRadius: 3 },
      ],
    },
    options: { responsive: true, scales: { y: { ticks: { callback: v => fmtMXN(v) } } } },
  });

  // Percepciones table
  fillTable('tbl-nom-perc', (d.by_percepcion || []).map(p => [
    p.tipo_percepcion,
    p.label || '—',
    fmtMXN(p.importe_gravado),
    fmtMXN(p.importe_exento),
    fmtMXN((p.importe_gravado||0) + (p.importe_exento||0)),
  ]));

  // Deducciones table
  fillTable('tbl-nom-ded', (d.by_deduccion || []).map(p => [
    p.tipo_deduccion,
    p.label || '—',
    fmtMXN(p.total_importe),
  ]));

  // Employees table
  fillTable('tbl-empleados', (d.by_employee || []).map(e => [
    `<code class="rfc">${e.employee_rfc || '—'}</code>`,
    e.nombre || '—',
    e.departamento || '—',
    e.puesto || '—',
    fmtMXN(e.total_mxn),
    fmtMXN2(e.avg_sdi),
    e.active_months,
  ]), true);
}

/* ══════════════════════════════════════════════════════════════════════════
   NORMALIZACIÓN
   ══════════════════════════════════════════════════════════════════════════ */

let _normRfc = '';
let _normDlType = '';

function wireNormForms(rfc, dlType) {
  _normRfc = rfc;
  _normDlType = dlType;

  // Refresh button
  document.getElementById('norm-refresh-btn')?.removeEventListener('click', onNormRefresh);
  document.getElementById('norm-refresh-btn')?.addEventListener('click', onNormRefresh);

  // Counterparty rule form
  const normForm = document.getElementById('norm-form');
  if (normForm) {
    const fresh = normForm.cloneNode(true);
    normForm.replaceWith(fresh);
    fresh.addEventListener('submit', onNormSubmit);
    // Re-wire action toggle
    fresh.querySelector('#norm-action')?.addEventListener('change', function() {
      const wrap = fresh.querySelector('#norm-group-name-wrap');
      if (wrap) wrap.style.display = this.value === 'group' ? '' : 'none';
    });
  }

  // Payroll norm form
  const pnForm = document.getElementById('payroll-norm-form');
  if (pnForm) {
    const fresh = pnForm.cloneNode(true);
    pnForm.replaceWith(fresh);
    fresh.addEventListener('submit', onPayrollNormSubmit);
    fresh.querySelector('#pnorm-family')?.addEventListener('change', function() {
      const wrap = fresh.querySelector('#pnorm-pct-wrap');
      if (wrap) wrap.style.display = this.value === 'scale_employee_pct' ? '' : 'none';
    });
  }
}

function onNormRefresh() {
  loadNormRules(_normRfc, _normDlType);
  loadPayrollNormRules(_normRfc);
}

async function loadNormRules(rfc, dlType) {
  const list = document.getElementById('norm-rules-list');
  if (!list) return;
  list.innerHTML = '<span class="an-loading-text">Cargando…</span>';
  try {
    const data = await fetchJSON(`/api/v1/analytics/${encodeURIComponent(rfc)}/normalization?dl_type=${dlType}`);
    const rules = data.rules || [];
    if (!rules.length) { list.innerHTML = '<p class="an-empty-msg">Sin reglas.</p>'; return; }
    list.innerHTML = rules.map(r => `
      <div class="an-rule-row">
        <span class="an-rule-info">
          <strong>${r.action}</strong>
          ${r.source_rfc ? `<code class="rfc">${r.source_rfc}</code>` : ''}
          ${r.source_name ? `<em>${r.source_name}</em>` : ''}
          ${r.group_name  ? `→ <span>${r.group_name}</span>` : ''}
        </span>
        <button class="btn btn-ghost btn-sm an-rule-del" data-id="${r.id}" data-kind="norm">✕</button>
      </div>`).join('');
    list.querySelectorAll('.an-rule-del[data-kind="norm"]').forEach(btn => {
      btn.addEventListener('click', () => deleteNormRule(rfc, btn.dataset.id));
    });
  } catch (err) {
    list.innerHTML = `<p class="stream-error">${err.message}</p>`;
  }
}

async function loadPayrollNormRules(rfc) {
  const list = document.getElementById('payroll-norm-rules-list');
  if (!list) return;
  list.innerHTML = '<span class="an-loading-text">Cargando…</span>';
  try {
    const data = await fetchJSON(`/api/v1/analytics/${encodeURIComponent(rfc)}/normalization/payroll`);
    const rules = data.rules || [];
    if (!rules.length) { list.innerHTML = '<p class="an-empty-msg">Sin reglas.</p>'; return; }
    list.innerHTML = rules.map(r => `
      <div class="an-rule-row">
        <span class="an-rule-info">
          <strong>${r.rule_family}</strong>
          ${r.employee_rfc ? `<code class="rfc">${r.employee_rfc}</code>` : ''}
          ${r.value_pct != null ? `${r.value_pct}%` : ''}
          ${r.period_start ? `${r.period_start}–${r.period_end || '…'}` : ''}
        </span>
        <button class="btn btn-ghost btn-sm an-rule-del" data-id="${r.id}" data-kind="payroll">✕</button>
      </div>`).join('');
    list.querySelectorAll('.an-rule-del[data-kind="payroll"]').forEach(btn => {
      btn.addEventListener('click', () => deletePayrollNormRule(rfc, btn.dataset.id));
    });
  } catch (err) {
    list.innerHTML = `<p class="stream-error">${err.message}</p>`;
  }
}

async function onNormSubmit(e) {
  e.preventDefault();
  const form   = e.currentTarget;
  const rfc    = _normRfc;
  const dlType = form.querySelector('#norm-dl-type')?.value || 'emitidos';
  const body   = {
    source_rfc:  form.querySelector('#norm-src-rfc')?.value.trim().toUpperCase() || null,
    source_name: form.querySelector('#norm-src-name')?.value.trim() || null,
    dl_type:     dlType,
    action:      form.querySelector('#norm-action')?.value || 'group',
    group_name:  form.querySelector('#norm-group-name')?.value.trim() || null,
  };
  try {
    await postJSON(`/api/v1/analytics/${encodeURIComponent(rfc)}/normalization`, body);
    form.reset();
    loadNormRules(rfc, dlType);
  } catch (err) {
    alert('Error: ' + err.message);
  }
}

async function onPayrollNormSubmit(e) {
  e.preventDefault();
  const form = e.currentTarget;
  const rfc  = _normRfc;
  const body = {
    employee_rfc: form.querySelector('#pnorm-emp-rfc')?.value.trim().toUpperCase() || null,
    rule_family:  form.querySelector('#pnorm-family')?.value || 'exclude_employee',
    value_pct:    parseFloat(form.querySelector('#pnorm-pct')?.value) || null,
    period_start: form.querySelector('#pnorm-from')?.value || null,
    period_end:   form.querySelector('#pnorm-to')?.value   || null,
    notes:        form.querySelector('#pnorm-notes')?.value.trim() || null,
  };
  try {
    await postJSON(`/api/v1/analytics/${encodeURIComponent(rfc)}/normalization/payroll`, body);
    form.reset();
    loadPayrollNormRules(rfc);
  } catch (err) {
    alert('Error: ' + err.message);
  }
}

async function deleteNormRule(rfc, id) {
  if (!confirm('¿Eliminar esta regla?')) return;
  try {
    await deleteReq(`/api/v1/analytics/${encodeURIComponent(rfc)}/normalization/${id}`);
    loadNormRules(rfc, _normDlType);
  } catch (err) {
    alert('Error: ' + err.message);
  }
}

async function deletePayrollNormRule(rfc, id) {
  if (!confirm('¿Eliminar esta regla?')) return;
  try {
    await deleteReq(`/api/v1/analytics/${encodeURIComponent(rfc)}/normalization/payroll/${id}`);
    loadPayrollNormRules(rfc);
  } catch (err) {
    alert('Error: ' + err.message);
  }
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || err.message || `HTTP ${res.status}`);
  }
  return res.json().catch(() => ({}));
}

async function deleteReq(url) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || err.message || `HTTP ${res.status}`);
  }
}

/* ══════════════════════════════════════════════════════════════════════════
   UTILS
   ══════════════════════════════════════════════════════════════════════════ */

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function truncate(str, maxLen) {
  if (!str) return '—';
  return str.length > maxLen ? str.slice(0, maxLen) + '…' : str;
}

/** Fill an invoice-table tbody. cells = array of arrays of (string|html). html=true allows innerHTML. */
function fillTable(id, rows, html = false) {
  const tbl = document.getElementById(id);
  if (!tbl) return;
  const tbody = tbl.querySelector('tbody');
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="99" style="text-align:center;color:var(--text-subtle)">Sin datos</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(r =>
    '<tr>' + r.map(c => `<td${html ? '' : ''}>${c}</td>`).join('') + '</tr>'
  ).join('');
}
