/* eslint-disable no-console */

async function getJSON(url, opts = {}) {
  const res = await fetch(url, opts);
  const text = await res.text();
  try {
    const data = JSON.parse(text);
    if (!res.ok) throw new Error(data.error || res.statusText);
    return data;
  } catch (e) {
    if (!res.ok) throw new Error(text || res.statusText);
    throw e;
  }
}

function setOutput(obj) {
  const pre = document.getElementById('output');
  pre.textContent = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
  pre.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

async function refreshHealth() {
  const pill = document.getElementById('health');
  try {
    const data = await getJSON('/health');
    const ok = data.status === 'ok' && String(data.db || '').startsWith('ok');
    pill.textContent = ok ? 'SantÃ©: OK' : `SantÃ©: ${data.db}`;
    pill.classList.toggle('ok', ok);
    pill.classList.toggle('bad', !ok);
  } catch (e) {
    pill.textContent = `SantÃ©: erreur`;
    pill.classList.remove('ok');
    pill.classList.add('bad');
  }
}

async function refreshOverview() {
  const nameEl = document.getElementById('db-name');
  const cont = document.getElementById('overview');
  if (!nameEl || !cont) return;
  try {
    const data = await getJSON('/db/tables');
    const db = (data.db && data.db.db) || '(non dÃ©fini)';
    nameEl.textContent = db || '(non dÃ©fini)';
    const tables = data.tables || {};
    cont.innerHTML = '';
    Object.keys(tables).sort().forEach((t) => {
      const card = document.createElement('a');
      card.className = 'card';
      card.href = `/table/${encodeURIComponent(t)}`;
      card.innerHTML = `<div class="card-title">${t}</div><div class="card-meta">${tables[t]} lignes</div>`;
      cont.appendChild(card);
    });
  } catch (e) {
    nameEl.textContent = '(erreur)';
    cont.textContent = 'Impossible de charger les tables';
  }
}

async function loadDbList() {
  const sel = document.getElementById('db-list');
  if (!sel) return;
  try {
    const data = await getJSON('/db/list');
    const arr = data.databases || [];
    sel.innerHTML = '';
    arr.forEach((name) => {
      const opt = document.createElement('option');
      opt.value = name; opt.textContent = name; sel.appendChild(opt);
    });
  } catch (e) {
    // ignore
  }
}

async function loadAllowedDepartments() {
  try {
    const res = await getJSON('/db/allowed');
    const allowed = res.departments || [];
    const ns = res.namespace || '';
    const inputDept = document.getElementById('dept');
    const selectDept = document.getElementById('dept-select');
    const inputDb = document.getElementById('dbname');
    const pickSel = document.getElementById('db-list');
    const pickBtn = document.getElementById('btn-pick-db');
    const nsBadge = document.getElementById('ns-badge');
    if (nsBadge) nsBadge.textContent = ns || '(par dÃ©faut)';
    const fillDeptSelects = (depts) => {
      const srcSel = document.getElementById('srcname-select');
      const dstSel = document.getElementById('dstname-select');
      if (srcSel && dstSel) {
        srcSel.innerHTML = '';
        dstSel.innerHTML = '';
        depts.forEach((d) => {
          const opt1 = document.createElement('option'); opt1.value = d; opt1.textContent = d; srcSel.appendChild(opt1);
          const opt2 = document.createElement('option'); opt2.value = d; opt2.textContent = d; dstSel.appendChild(opt2);
        });
        // Show selects, hide text inputs
        const srcInput = document.getElementById('srcname');
        const dstInput = document.getElementById('dstname');
        srcSel.classList.remove('hidden');
        dstSel.classList.remove('hidden');
        if (srcInput) srcInput.classList.add('hidden');
        if (dstInput) dstInput.classList.add('hidden');
      }
    };

    if (allowed.length > 0) {
      // Lock inputs: only allowed departments
      if (inputDept) { inputDept.classList.add('hidden'); }
      if (selectDept) {
        selectDept.classList.remove('hidden');
        selectDept.innerHTML = '';
        allowed.forEach((d) => {
          const opt = document.createElement('option');
          opt.value = d; opt.textContent = d; selectDept.appendChild(opt);
        });
      }
      if (inputDb) { inputDb.disabled = true; inputDb.placeholder = `verrouillÃ© (namespace ${ns})`; }
      if (pickSel) {
        pickSel.innerHTML = '';
        allowed.forEach((d) => {
          const qname = `${ns}_${d}`;
          const opt = document.createElement('option');
          opt.value = qname; opt.textContent = qname; pickSel.appendChild(opt);
        });
      }
      fillDeptSelects(allowed);
    } else {
      // No explicit whitelist: derive department names from db-list options if available
      const depts = [];
      if (pickSel && pickSel.options && pickSel.options.length) {
        const prefix = ns ? ns + '_' : '';
        for (const opt of pickSel.options) {
          const name = opt.value || '';
          if (prefix && name.startsWith(prefix)) {
            depts.push(name.slice(prefix.length));
          }
        }
      }
      if (depts.length) fillDeptSelects(depts);
    }
  } catch (e) {
    // ignore
  }
}

async function onLoadCSVs() {
  const btn = document.getElementById('btn-load');
  await withBusy(btn, 'Chargementâ€¦', async () => {
    setOutput('Chargement en coursâ€¦');
    try {
      const prefix = (document.getElementById('prefix')?.value || '').trim();
      const url = prefix ? `/load-csvs?prefix=${encodeURIComponent(prefix)}` : '/load-csvs';
      const res = await getJSON(url, { method: 'POST' });
      setOutput(res);
      showToast('CSV chargÃ©s avec succÃ¨s', 'success');
      await refreshOverview();
    } catch (e) {
      showToast(`Erreur chargement: ${String(e)}`, 'error');
      setOutput({ error: String(e) });
    }
  });
}

async function onCreateDB() {
  const name = document.getElementById('dbname').value.trim();
  if (!name) { setOutput({ error: 'Veuillez saisir un nom de base' }); return; }
  const btn = document.getElementById('btn-create-db');
  await withBusy(btn, 'CrÃ©ationâ€¦', async () => {
    setOutput('CrÃ©ation de la base et des tablesâ€¦');
    try {
      const res = await getJSON(`/db/create-and-init?name=${encodeURIComponent(name)}`);
      showToast(`Base ${name} prÃªte`, 'success');
      setOutput(res);
      await refreshOverview();
    } catch (e) {
      showToast(`Erreur crÃ©ation base: ${String(e)}`, 'error');
      setOutput({ error: String(e) });
    }
  });
}

async function onSwitchDB() {
  const name = document.getElementById('dbname').value.trim();
  if (!name) { setOutput({ error: 'Veuillez saisir un nom de base' }); return; }
  const btn = document.getElementById('btn-switch-db');
  await withBusy(btn, 'Basculementâ€¦', async () => {
    setOutput('Basculement sur la base en coursâ€¦');
    try {
      const res = await getJSON(`/db/switch?name=${encodeURIComponent(name)}`);
      showToast(`ConnectÃ© Ã  ${name}`, 'success');
      setOutput(res);
      await refreshOverview();
    } catch (e) {
      showToast(`Erreur bascule: ${String(e)}`, 'error');
      setOutput({ error: String(e) });
    }
  });
}

async function onInitDB() {
  const btn = document.getElementById('btn-init');
  await withBusy(btn, 'CrÃ©ationâ€¦', async () => {
    setOutput('CrÃ©ation des tables (models)â€¦');
    try {
      const res = await getJSON('/init-db', { method: 'POST' });
      showToast('Tables modÃ¨les crÃ©Ã©es', 'success');
      setOutput(res);
      await refreshOverview();
    } catch (e) {
      showToast(`Erreur crÃ©ation tables: ${String(e)}`, 'error');
      setOutput({ error: String(e) });
    }
  });
}

async function onAssign() {
  const btn = document.getElementById('btn-assign');
  await withBusy(btn, 'Affectationâ€¦', async () => {
    const start = document.getElementById('start').value.trim();
    const end = document.getElementById('end').value.trim();
    const max = document.getElementById('max').value.trim();
    const payload = {};
    if (start) payload.start = start;
    if (end) payload.end = end;
    if (max) payload.max = Number(max);
    setOutput('Affectation en coursâ€¦');
    try {
      const res = await getJSON('/assign', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      showToast('Affectation terminÃ©e', 'success');
      setOutput(res);
    } catch (e) {
      showToast(`Erreur affectation: ${String(e)}`, 'error');
      setOutput({ error: String(e) });
    }
  });
}

window.addEventListener('DOMContentLoaded', () => {
  const FILE_ONLY = (window.FILE_ONLY === true || window.FILE_ONLY === 'true');
  const elLoad = document.getElementById('btn-load'); if (elLoad) elLoad.addEventListener('click', onLoadCSVs);
  const elInit = document.getElementById('btn-init'); if (elInit) elInit.addEventListener('click', onInitDB);
  const elCreate = document.getElementById('btn-create-db'); if (elCreate) elCreate.addEventListener('click', onCreateDB);
  const elSwitch = document.getElementById('btn-switch-db'); if (elSwitch) elSwitch.addEventListener('click', onSwitchDB);
  const elAssign = document.getElementById('btn-assign'); if (elAssign) elAssign.addEventListener('click', onAssign);
  const elDownload = document.getElementById('btn-download'); if (elDownload) elDownload.addEventListener('click', () => {
    // Navigate to the download endpoint; browser will handle the file
    window.location.href = '/download/assigned';
  });
  refreshHealth();
  if (!FILE_ONLY) {
    refreshOverview();
    loadDbList();
  }
  loadAllowedDepartments();
  const pickBtn = document.getElementById('btn-pick-db');
  const refreshBtn = document.getElementById('btn-refresh-db');
  if (pickBtn) pickBtn.addEventListener('click', async () => {
    const sel = document.getElementById('db-list');
    const name = sel && sel.value;
    if (!name) return;
    document.getElementById('dbname').value = name;
    await onSwitchDB();
  });
  if (refreshBtn) refreshBtn.addEventListener('click', loadDbList);
  const dropBtn = document.getElementById('btn-drop-db');
  if (dropBtn) dropBtn.addEventListener('click', async () => {
    const sel = document.getElementById('db-list');
    const name = sel && sel.value;
    if (!name) { showToast('SÃ©lectionnez une base', 'error'); return; }
    if (!window.confirm(`Voulez-vous vraiment supprimer la base â€œ${name}â€ ?\nCette action est irrÃ©versible.`)) return;
    await withBusy(dropBtn, 'Suppressionâ€¦', async () => {
      try {
        const res = await getJSON(`/db/drop?name=${encodeURIComponent(name)}`, { method: 'POST' });
        setOutput(res);
        showToast(`Base supprimÃ©e: ${res.dropped}`, 'success');
        await loadDbList();
        await refreshOverview();
      } catch (e) {
        setOutput({ error: String(e) });
        showToast(`Erreur suppression: ${String(e)}`, 'error');
      }
    });
  });
  const copyBtn = document.getElementById('btn-copy-schema');
  if (copyBtn) copyBtn.addEventListener('click', async () => {
    const src = (document.getElementById('srcname-select')?.value || document.getElementById('srcname')?.value || '').trim();
    const dst = (document.getElementById('dstname-select')?.value || document.getElementById('dstname')?.value || '').trim();
    if (!src || !dst) { showToast('Renseignez source et cible', 'error'); return; }
    await withBusy(copyBtn, 'Duplicationâ€¦', async () => {
      try {
        const res = await getJSON(`/db/copy-schema?source=${encodeURIComponent(src)}&target=${encodeURIComponent(dst)}`);
        setOutput(res);
        showToast(`SchÃ©ma copiÃ© de ${src} vers ${dst}`, 'success');
      } catch (e) {
        setOutput({ error: String(e) });
        showToast(`Erreur duplication: ${String(e)}`, 'error');
      }
    });
  });

  // Mode simple
  const btnSimpleLoad = document.getElementById('btn-simple-load');
  if (btnSimpleLoad) btnSimpleLoad.addEventListener('click', async () => {
    const dept = (document.getElementById('dept-select')?.value || document.getElementById('dept')?.value || '').trim();
    if (!dept) { showToast('Saisissez un nom de dÃ©partement', 'error'); return; }
    await withBusy(btnSimpleLoad, 'Chargementâ€¦', async () => {
      try {
        const res = await getJSON(`/simple/load?dept=${encodeURIComponent(dept)}`, { method: 'POST' });
        setOutput(res);
        showToast(`CSV chargÃ©s pour ${res.db}`, 'success');
        await refreshOverview();
      } catch (e) {
        setOutput({ error: String(e) });
        showToast(`Erreur: ${String(e)}`, 'error');
      }
    });
  });

  const btnSimpleAssign = document.getElementById('btn-simple-assign');
  if (btnSimpleAssign) btnSimpleAssign.addEventListener('click', async () => {
    const dept = (document.getElementById('dept-select')?.value || document.getElementById('dept')?.value || '').trim();
    await withBusy(btnSimpleAssign, 'Affectationâ€¦', async () => {
      try {
        let res;
        if (FILE_ONLY && dept) {
          res = await getJSON(`/departments/${encodeURIComponent(dept)}/assign`, { method: 'POST' });
        } else {
          res = await getJSON('/assign', { method: 'POST' });
        }
        setOutput(res);
        showToast('Affectation terminÃ©e â€“ tÃ©lÃ©chargementâ€¦', 'success');
        // DÃ©clenche le tÃ©lÃ©chargement du fichier gÃ©nÃ©rÃ©
        if (FILE_ONLY && dept) {
          // prefer department output file path
          window.location.href = `/download?dept=${encodeURIComponent(dept)}`;
        } else {
          window.location.href = '/download';
        }
      } catch (e) {
        setOutput({ error: String(e) });
        showToast(`Erreur affectation: ${String(e)}`, 'error');
      }
    });
  });

  // Hide DB-related UI when file-only mode
  if (FILE_ONLY) {
    const hideClosestSectionOf = (sel) => {
      const el = document.querySelector(sel);
      if (el) {
        let node = el;
        while (node && node.tagName && node.tagName.toLowerCase() !== 'section') node = node.parentElement;
        if (node) node.style.display = 'none';
      }
    };
    hideClosestSectionOf('#overview');
    hideClosestSectionOf('#db-list');
    hideClosestSectionOf('#prefix');
    document.querySelectorAll('.db-form').forEach(f => { f.style.display = 'none'; });
    ['btn-load','btn-init','btn-create-db','btn-switch-db','btn-copy-schema','btn-pick-db','btn-refresh-db','btn-drop-db'].forEach(id => {
      const b = document.getElementById(id); if (b) { b.disabled = true; b.style.display = 'none'; }
    });
  }
});
let toastTimer = null;
function showToast(message, type = 'info') {
  const el = document.getElementById('toast');
  if (!el) return;
  el.className = `toast ${type}`;
  el.textContent = message;
  // Force reflow to restart animation when called rapidly
  void el.offsetWidth; // eslint-disable-line no-unused-expressions
  el.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    el.classList.remove('show');
  }, 3000);
}

function withBusy(btn, labelBusy, fn) {
  const prev = btn.textContent;
  btn.disabled = true;
  if (labelBusy) btn.textContent = labelBusy;
  const done = () => { btn.disabled = false; btn.textContent = prev; };
  return fn().then((res) => { done(); return res; }).catch((e) => { done(); throw e; });
}


