/* ─── Pulso SAT — app.js ─────────────────────────────────────────────────── */
'use strict';

/* ══════════════════════════════════════════════════════════════════════════
   FORM PAGE — shared helpers
   ══════════════════════════════════════════════════════════════════════════ */

/* ── Auth tab switching ───────────────────────────────────────────────── */
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
    btn.classList.add('active');
    document.getElementById('panel-' + tab)?.classList.remove('hidden');
    document.getElementById('authTypeInput').value = tab;
  });
});

/* ── File upload zones ────────────────────────────────────────────────── */
function initUploadZone(inputId, hintId, zoneId) {
  const input = document.getElementById(inputId);
  const hint  = document.getElementById(hintId);
  const zone  = document.getElementById(zoneId);
  if (!input || !hint || !zone) return;

  const update = file => {
    if (file) {
      hint.textContent = file.name;
      zone.classList.add('has-file');
    } else {
      hint.textContent = 'Arrastra o haz clic para seleccionar';
      zone.classList.remove('has-file');
    }
  };

  input.addEventListener('change', () => update(input.files[0] || null));
  zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('dragover'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('dragover');
    const file = e.dataTransfer?.files[0];
    if (file) {
      const dt = new DataTransfer();
      dt.items.add(file);
      input.files = dt.files;
      update(file);
    }
  });
}

initUploadZone('cert_file', 'cert-name', 'zone-cert');
initUploadZone('key_file',  'key-name',  'zone-key');

/* ── Password toggle ──────────────────────────────────────────────────── */
document.querySelectorAll('.toggle-pw').forEach(btn => {
  btn.addEventListener('click', () => {
    const inp = document.getElementById(btn.dataset.target);
    if (inp) inp.type = inp.type === 'password' ? 'text' : 'password';
  });
});

/* ── RFC → uppercase ──────────────────────────────────────────────────── */
document.getElementById('rfc')?.addEventListener('input', function () {
  const pos = this.selectionStart;
  this.value = this.value.toUpperCase();
  this.setSelectionRange(pos, pos);
});

/* ── Default dates (current month) ───────────────────────────────────── */
(function setDefaultDates() {
  const fromEl = document.getElementById('period_from');
  const toEl   = document.getElementById('period_to');
  if (!fromEl || !toEl) return;
  const now  = new Date();
  const y    = now.getFullYear();
  const m    = String(now.getMonth() + 1).padStart(2, '0');
  const last = new Date(y, now.getMonth() + 1, 0).getDate();
  fromEl.value = `${y}-${m}-01`;
  toEl.value   = `${y}-${m}-${last}`;
})();

/* ── File → base64 helper ─────────────────────────────────────────────── */
function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload  = () => {
      // result is "data:<mime>;base64,<data>" — we want only the data part
      const b64 = reader.result.split(',')[1];
      resolve(b64);
    };
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

/* ══════════════════════════════════════════════════════════════════════════
   STREAMING QUERY
   ══════════════════════════════════════════════════════════════════════════ */

let activeReader   = null;   // current ReadableStreamDefaultReader
let streamTotal    = 0;
let streamDlType   = 'emitidos';
window.__captchaSessionId = null;   // session ID of the pending captcha challenge

/* ── Grab DOM references ──────────────────────────────────────────────── */
const queryForm      = document.getElementById('queryForm');
const submitBtn      = document.getElementById('submitBtn');
const btnLabel       = submitBtn?.querySelector('.btn-label');
const btnSpinner     = submitBtn?.querySelector('.btn-spinner');
const streamSection  = document.getElementById('streamSection');
const streamCount    = document.getElementById('streamCount');
const streamBadge    = document.getElementById('streamBadge');
const streamPeriod   = document.getElementById('streamPeriodLabel');
const streamTableBody = document.getElementById('streamTableBody');
const streamSearch   = document.getElementById('streamSearch');
const streamError    = document.getElementById('streamError');
const streamCancel   = document.getElementById('streamCancelBtn');
const streamCheckAll   = document.getElementById('streamCheckAll');
const streamDlSelected = document.getElementById('streamDlSelected');
const streamSelectAll  = document.getElementById('streamSelectAll');

/* ── Form intercept — streaming fetch ────────────────────────────────── */
queryForm?.addEventListener('submit', async e => {
  e.preventDefault();

  // Cancel any previous stream
  if (activeReader) { try { await activeReader.cancel(); } catch (_) {} }

  // Reset UI
  streamTotal = 0;
  if (streamTableBody) streamTableBody.innerHTML = '';
  if (streamError)     { streamError.textContent = ''; streamError.classList.add('hidden'); }
  if (streamSection)   streamSection.classList.add('hidden');
  if (streamCheckAll)  streamCheckAll.checked = false;
  updateBulkBtn();

  // Loading state
  if (submitBtn)  submitBtn.disabled = true;
  if (btnLabel)   btnLabel.classList.add('hidden');
  if (btnSpinner) btnSpinner.classList.remove('hidden');

  // Read form values
  const authType    = document.getElementById('authTypeInput')?.value || 'fiel';
  const periodFrom  = document.getElementById('period_from')?.value || '';
  const periodTo    = document.getElementById('period_to')?.value   || '';
  const dlType      = document.getElementById('download_type')?.value || 'emitidos';
  streamDlType = dlType;

  // Build auth payload
  let auth;
  try {
    if (authType === 'fiel') {
      const certFile = document.getElementById('cert_file')?.files[0];
      const keyFile  = document.getElementById('key_file')?.files[0];
      if (!certFile) throw new Error('Selecciona el certificado (.cer)');
      if (!keyFile)  throw new Error('Selecciona la clave privada (.key)');
      const [certB64, keyB64] = await Promise.all([fileToBase64(certFile), fileToBase64(keyFile)]);
      auth = {
        type:        'fiel',
        certificate: certB64,
        private_key: keyB64,
        password:    document.getElementById('fiel_password')?.value || '',
      };
    } else {
      auth = {
        type:     'ciec',
        rfc:      document.getElementById('rfc')?.value || '',
        password: document.getElementById('ciec_password')?.value || '',
      };
    }
  } catch (err) {
    resetSubmitBtn();
    showStreamError(err.message);
    return;
  }

  // Store credentials globally so download buttons can reuse them
  window.__CREDS__         = auth;
  window.__DOWNLOAD_TYPE__ = dlType;

  // Update period label
  if (streamPeriod) {
    streamPeriod.innerHTML =
      `<span class="summary-num">${periodFrom}</span>
       <span class="summary-label">–</span>
       <span class="summary-num">${periodTo}</span>`;
  }

  // Open stream
  let response;
  try {
    response = await fetch('/api/v1/invoices/list/stream', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ auth, period_from: periodFrom, period_to: periodTo, download_type: dlType }),
    });
  } catch (err) {
    resetSubmitBtn();
    showStreamError('No se pudo conectar con el servidor: ' + err.message);
    return;
  }

  if (!response.ok) {
    resetSubmitBtn();
    const err = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
    showStreamError(err.error || `Error ${response.status}`);
    return;
  }

  // Show results section and start reading
  if (streamSection) streamSection.classList.remove('hidden');
  resetSubmitBtn();

  activeReader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await activeReader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE events are separated by "\n\n"
      const events = buffer.split('\n\n');
      buffer = events.pop(); // keep the incomplete trailing chunk

      for (const event of events) {
        if (!event.startsWith('data: ')) continue;
        const jsonStr = event.slice(6).trim();
        if (!jsonStr) continue;

        let data;
        try { data = JSON.parse(jsonStr); } catch (_) { continue; }

        if (data.__done__) {
          markStreamDone(data.total ?? streamTotal);
        } else if (data.__captcha__) {
          showCaptchaModal(data.session_id, data.image_base64, data.mime);
        } else {
          appendInvoiceRow(data);
        }
      }
    }
  } catch (err) {
    if (err.name !== 'AbortError') showStreamError('Stream interrumpido: ' + err.message);
  } finally {
    activeReader = null;
    // If we never got a __done__ event (e.g. PHP crashed), mark as done anyway
    markStreamDone(streamTotal);
  }
});

/* ── Cancel button ────────────────────────────────────────────────────── */
streamCancel?.addEventListener('click', async () => {
  if (activeReader) { try { await activeReader.cancel(); } catch (_) {} activeReader = null; }
  markStreamDone(streamTotal);
});

/* ── Reset submit button ──────────────────────────────────────────────── */
function resetSubmitBtn() {
  if (submitBtn)  submitBtn.disabled = false;
  if (btnLabel)   btnLabel.classList.remove('hidden');
  if (btnSpinner) btnSpinner.classList.add('hidden');
}

/* ── Mark stream as complete ──────────────────────────────────────────── */
function markStreamDone(total) {
  if (!streamBadge) return;
  streamBadge.innerHTML = `<svg class="icon-sm" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg> ${total} facturas`;
  streamBadge.className = 'badge badge-green';
  streamCancel?.classList.add('hidden');
}

/* ── Show stream error ────────────────────────────────────────────────── */
function showStreamError(msg) {
  if (!streamError) return;
  streamError.textContent = '⚠ ' + msg;
  streamError.classList.remove('hidden');
  if (streamSection) streamSection.classList.remove('hidden');
}

/* ── Append one invoice row ───────────────────────────────────────────── */
function statusBadge(state) {
  const s = (state || '').toLowerCase();
  if (s === 'vigente')   return `<span class="badge badge-green">Vigente</span>`;
  if (s === 'cancelado') return `<span class="badge badge-red">Cancelado</span>`;
  return `<span class="badge badge-gray">${state || '—'}</span>`;
}

function appendInvoiceRow(inv) {
  if (!streamTableBody) return;
  streamTotal++;
  if (streamCount) streamCount.textContent = streamTotal;

  const uuid    = inv.uuid || '';
  const short   = uuid.length > 13 ? uuid.slice(0, 13) + '…' : uuid;
  const total   = inv.total ? '$' + parseFloat(inv.total).toLocaleString('es-MX', { minimumFractionDigits: 2 }) : '—';

  const tr = document.createElement('tr');
  tr.classList.add('row-new');
  tr.innerHTML = `
    <td class="col-check"><input type="checkbox" class="row-check" data-uuid="${uuid}" /></td>
    <td>
      <span class="uuid-cell" title="${uuid}" data-copy="${uuid}">
        ${short}
        <svg class="copy-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
      </span>
    </td>
    <td><code class="rfc">${inv.rfcEmisor || '—'}</code></td>
    <td class="nombre">${inv.nombreEmisor || '—'}</td>
    <td><code class="rfc">${inv.rfcReceptor || '—'}</code></td>
    <td class="fecha">${inv.fechaEmision || '—'}</td>
    <td class="num">${total}</td>
    <td>${statusBadge(inv.estadoComprobante)}</td>
    <td class="col-actions">
      <button class="btn-icon" data-dl-uuid="${uuid}" data-dl-type="xml" title="Descargar XML">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        XML
      </button>
      <button class="btn-icon btn-icon-pdf" data-dl-uuid="${uuid}" data-dl-type="pdf" title="Descargar PDF">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        PDF
      </button>
    </td>`;

  // UUID copy
  tr.querySelector('.uuid-cell')?.addEventListener('click', () => {
    navigator.clipboard.writeText(uuid).then(() => {
      const el = tr.querySelector('.uuid-cell');
      el.style.color = 'var(--success)';
      setTimeout(() => { el.style.color = ''; }, 1200);
    });
  });

  // Download buttons
  tr.querySelectorAll('[data-dl-uuid]').forEach(btn => {
    btn.addEventListener('click', () => downloadInvoices([btn.dataset.dlUuid], btn.dataset.dlType || 'xml'));
  });

  // Checkbox
  tr.querySelector('.row-check')?.addEventListener('change', updateBulkBtn);

  // Remove highlight class after animation
  tr.addEventListener('animationend', () => tr.classList.remove('row-new'), { once: true });

  streamTableBody.appendChild(tr);
}

/* ── Live search ──────────────────────────────────────────────────────── */
streamSearch?.addEventListener('input', () => {
  const q = streamSearch.value.toLowerCase().trim();
  document.querySelectorAll('#streamTableBody tr').forEach(row => {
    row.style.display = (q && !row.textContent.toLowerCase().includes(q)) ? 'none' : '';
  });
});

/* ── Checkbox bulk select ─────────────────────────────────────────────── */
function updateBulkBtn() {
  const checked = document.querySelectorAll('#streamTableBody .row-check:checked');
  if (streamDlSelected) streamDlSelected.disabled = checked.length === 0;
  if (streamCheckAll) {
    const all = document.querySelectorAll('#streamTableBody .row-check');
    streamCheckAll.checked = all.length > 0 && checked.length === all.length;
    streamCheckAll.indeterminate = checked.length > 0 && checked.length < all.length;
  }
}

streamCheckAll?.addEventListener('change', () => {
  document.querySelectorAll('#streamTableBody .row-check').forEach(c => { c.checked = streamCheckAll.checked; });
  updateBulkBtn();
});

streamSelectAll?.addEventListener('click', () => {
  const all = [...document.querySelectorAll('#streamTableBody .row-check')];
  const allChecked = all.every(c => c.checked);
  all.forEach(c => { c.checked = !allChecked; });
  if (streamCheckAll) streamCheckAll.checked = !allChecked;
  updateBulkBtn();
});

streamDlSelected?.addEventListener('click', () => {
  const uuids = [...document.querySelectorAll('#streamTableBody .row-check:checked')]
    .map(c => c.dataset.uuid).filter(Boolean);
  if (uuids.length) downloadInvoices(uuids, 'xml');
});

/* ══════════════════════════════════════════════════════════════════════════
   DOWNLOAD (used by both streaming table and invoices.html table)
   ══════════════════════════════════════════════════════════════════════════ */

const toast    = document.getElementById('dlToast');
const toastMsg = document.getElementById('dlToastMsg');

function showToast(msg) {
  if (!toast || !toastMsg) return;
  toastMsg.textContent = msg;
  toast.classList.remove('hidden');
}
function hideToast() { toast?.classList.add('hidden'); }

async function downloadInvoices(uuids, resourceType) {
  const creds  = window.__CREDS__;
  const dlType = window.__DOWNLOAD_TYPE__ || 'emitidos';
  if (!creds) { alert('Credenciales no disponibles. Vuelve a hacer la consulta.'); return; }

  showToast(`Descargando ${uuids.length} factura(s) en ${resourceType.toUpperCase()}…`);

  try {
    const res = await fetch('/api/v1/invoices/download/stream', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ auth: creds, uuids, download_type: dlType, resource_type: resourceType }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Error desconocido' }));
      throw new Error(err.error || `HTTP ${res.status}`);
    }

    const reader  = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer    = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const events = buffer.split('\n\n');
      buffer = events.pop();

      for (const event of events) {
        if (!event.startsWith('data: ')) continue;
        let data;
        try { data = JSON.parse(event.slice(6).trim()); } catch (_) { continue; }

        if (data.__captcha__) {
          showCaptchaModal(data.session_id, data.image_base64, data.mime);

        } else if (data.__download__) {
          // Decode base64 → Blob → trigger save
          const bytes  = Uint8Array.from(atob(data.data_b64), c => c.charCodeAt(0));
          const blob   = new Blob([bytes], { type: data.content_type });
          const url    = URL.createObjectURL(blob);
          const a      = document.createElement('a');
          a.href       = url;
          a.download   = data.filename;
          a.click();
          URL.revokeObjectURL(url);
          hideToast();

        } else if (data.__error__) {
          throw new Error(data.__error__);
        }
      }
    }
  } catch (err) {
    hideToast();
    alert(`Error al descargar: ${err.message}`);
  }
}

/* ── invoices.html page: wire existing download buttons ──────────────── */
document.querySelectorAll('[data-dl-uuid]').forEach(btn => {
  btn.addEventListener('click', () =>
    downloadInvoices([btn.dataset.dlUuid], btn.dataset.dlType || 'xml'));
});
document.getElementById('downloadSelectedBtn')?.addEventListener('click', () => {
  const uuids = [...document.querySelectorAll('.row-check:checked')]
    .map(c => c.dataset.uuid).filter(Boolean);
  if (uuids.length) downloadInvoices(uuids, 'xml');
});

/* ── invoices.html: UUID copy ─────────────────────────────────────────── */
document.querySelectorAll('.uuid-cell').forEach(el => {
  el.addEventListener('click', () => {
    navigator.clipboard.writeText(el.dataset.copy || '').then(() => {
      el.style.color = 'var(--success)';
      setTimeout(() => { el.style.color = ''; }, 1200);
    });
  });
});

/* ── invoices.html: table search ──────────────────────────────────────── */
document.getElementById('tableSearch')?.addEventListener('input', function () {
  const q = this.value.toLowerCase().trim();
  document.querySelectorAll('#invoiceTable tbody tr').forEach(row => {
    row.classList.toggle('hidden-row', q.length > 0 && !row.textContent.toLowerCase().includes(q));
  });
});

/* ══════════════════════════════════════════════════════════════════════════
   CAPTCHA MODAL
   ══════════════════════════════════════════════════════════════════════════ */

function showCaptchaModal(sessionId, imageBase64, mime) {
  window.__captchaSessionId = sessionId;

  const modal   = document.getElementById('captchaModal');
  const img     = document.getElementById('captchaImg');
  const input   = document.getElementById('captchaInput');
  const errorEl = document.getElementById('captchaError');

  img.src   = `data:${mime || 'image/gif'};base64,${imageBase64}`;
  img.alt   = 'Captcha del SAT';
  input.value = '';
  errorEl.classList.add('hidden');
  modal.classList.remove('hidden');

  // Focus input after the image renders
  requestAnimationFrame(() => input.focus());
}

function hideCaptchaModal() {
  document.getElementById('captchaModal')?.classList.add('hidden');
  window.__captchaSessionId = null;
}

async function submitCaptcha() {
  const input   = document.getElementById('captchaInput');
  const btn     = document.getElementById('captchaSubmitBtn');
  const errorEl = document.getElementById('captchaError');
  const answer  = input?.value?.trim();

  if (!answer) {
    input?.focus();
    return;
  }
  if (!window.__captchaSessionId) return;

  btn.disabled = true;
  errorEl.classList.add('hidden');

  try {
    const res = await fetch('/api/v1/invoices/captcha/solve', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ session_id: window.__captchaSessionId, answer }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Error desconocido' }));
      throw new Error(err.error || `HTTP ${res.status}`);
    }

    hideCaptchaModal();
  } catch (err) {
    errorEl.textContent = 'Error al enviar: ' + err.message;
    errorEl.classList.remove('hidden');
  } finally {
    btn.disabled = false;
  }
}

document.getElementById('captchaSubmitBtn')?.addEventListener('click', submitCaptcha);

document.getElementById('captchaInput')?.addEventListener('keydown', e => {
  if (e.key === 'Enter') submitCaptcha();
});

// Close modal on backdrop click
document.getElementById('captchaModal')?.addEventListener('click', e => {
  if (e.target === e.currentTarget) hideCaptchaModal();
});
