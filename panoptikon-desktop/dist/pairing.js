const invoke = window.__TAURI__?.core?.invoke ?? (async () => { throw new Error('Desktop bridge is unavailable.'); });
const requestsNode = document.getElementById('requests');
const errorNode = document.getElementById('error');
const known = new Map();
const finishing = new Set();
const selectedMappings = new Map();
let closing = false;
let renderedState = '';

function fail(error) {
  errorNode.hidden = false;
  errorNode.textContent = String(error);
}

async function closeWindow() {
  if (closing) return;
  closing = true;
  await invoke('close_relay_pairing_window');
}

async function copyText(value, button) {
  const text = value.trim();
  if (!text) return;
  try {
    let copied = false;
    if (navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(text);
        copied = true;
      } catch (_) {
        // Some WebView configurations expose the API but deny it. Fall back
        // to the selection-based copy path before reporting a failure.
      }
    }
    if (!copied) {
      const fallback = document.createElement('textarea');
      fallback.value = text;
      fallback.setAttribute('readonly', '');
      fallback.style.position = 'fixed';
      fallback.style.opacity = '0';
      document.body.append(fallback);
      try {
        fallback.select();
        copied = document.execCommand('copy');
      } finally {
        fallback.remove();
      }
    }
    if (!copied) throw new Error('Clipboard access was denied.');
    button.classList.remove('is-copied');
    void button.offsetWidth;
    button.classList.add('is-copied');
    button.querySelector('.copy-label').textContent = 'Copied';
    clearTimeout(button.copyReset);
    button.copyReset = setTimeout(() => {
      button.classList.remove('is-copied');
      button.querySelector('.copy-label').textContent = 'Copy';
    }, 1500);
  } catch (error) {
    fail(error);
  }
}

function requestCard(item, state) {
  const card = document.createElement('article'); card.className = 'pairing-request';
  const title = document.createElement('h2'); title.textContent = 'Connection request'; card.append(title);
  const details = document.createElement('dl'); details.className = 'request-details';
  for (const [label, value] of [['Server', item.name], ['Endpoint', item.origin], ['Address', item.server_url]]) {
    const term = document.createElement('dt'); term.textContent = label;
    const detail = document.createElement('dd'); detail.textContent = value;
    details.append(term, detail);
  }
  card.append(details);
  const rootsTitle = document.createElement('p'); rootsTitle.className = 'muted'; rootsTitle.textContent = 'These server folders are suggestions. Edit them as needed, match available folders on this computer, or leave them blank.'; card.append(rootsTitle);
  const mappings = selectedMappings.get(item.id) ?? new Map(); selectedMappings.set(item.id, mappings);
  const roots = document.createElement('div'); roots.className = 'pairing-mappings';
  if (!item.roots.length) { const empty = document.createElement('p'); empty.className = 'muted'; empty.textContent = 'No folder hints were supplied. You can add mappings later in Settings.'; roots.append(empty); }
  for (const root of item.roots) {
    const draft = mappings.get(root) ?? { remote: root, local: '' }; mappings.set(root, draft);
    const row = document.createElement('div'); row.className = 'pairing-mapping';
    const remoteLabel = document.createElement('label'); remoteLabel.className = 'mapping-label'; remoteLabel.textContent = 'Server folder';
    const remoteControls = document.createElement('div'); remoteControls.className = 'remote-controls';
    const remote = document.createElement('input'); remote.value = draft.remote; remote.spellcheck = false;
    remote.setAttribute('aria-label', `Server folder suggested as ${root}`);
    remote.oninput = () => { draft.remote = remote.value; };
    const copy = document.createElement('button'); copy.type = 'button'; copy.className = 'copy-root';
    copy.title = 'Copy server folder'; copy.setAttribute('aria-label', `Copy server folder ${root}`);
    copy.innerHTML = '<span class="copy-icon" aria-hidden="true"><svg class="copy-glyph" viewBox="0 0 24 24"><rect x="8" y="8" width="10" height="10" rx="2"></rect><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"></path></svg><svg class="check-glyph" viewBox="0 0 24 24"><path d="m5 12 4 4L19 6"></path></svg></span><span class="copy-label">Copy</span>';
    copy.onclick = () => copyText(remote.value, copy);
    remoteControls.append(remote, copy);
    const localLabel = document.createElement('label'); localLabel.className = 'mapping-label'; localLabel.textContent = 'Folder on this computer';
    const local = document.createElement('input'); local.placeholder = 'Matching folder on this computer'; local.value = draft.local;
    local.setAttribute('aria-label', `Matching folder on this computer for ${root}`);
    local.oninput = () => { draft.local = local.value; };
    const choose = document.createElement('button'); choose.type = 'button'; choose.textContent = 'Choose folder…'; choose.setAttribute('aria-label', `Choose matching folder for ${root}`); choose.onclick = async () => { const folder = await invoke('choose_relay_pairing_folder'); if (folder) { local.value = folder; draft.local = folder; } };
    row.append(remoteLabel, remoteControls, localLabel, local, choose); roots.append(row);
  }
  card.append(roots);
  if (state === 'pending') {
    const actions = document.createElement('div'); actions.className = 'pairing-actions';
    const reject = document.createElement('button'); reject.className = 'danger'; reject.textContent = 'Reject';
    reject.onclick = async () => { try { reject.disabled = true; approve.disabled = true; await invoke('relay_reject', { requestId: item.id }); await closeWindow(); } catch (error) { reject.disabled = false; approve.disabled = false; fail(error); } };
    const approve = document.createElement('button'); approve.className = 'primary'; approve.textContent = 'Approve pairing';
    approve.onclick = async () => { try { approve.disabled = true; reject.disabled = true; const mappingValues = [...mappings.values()].filter(({ remote, local }) => remote.trim() && local.trim()).map(({ remote, local }) => ({ remote: remote.trim(), local: local.trim() })); await invoke('relay_approve', { requestId: item.id, mappings: mappingValues }); finishing.add(item.id); renderedState = ''; await refresh(); } catch (error) { approve.disabled = false; reject.disabled = false; fail(error); } };
    actions.append(reject, approve); card.append(actions);
  } else {
    const status = document.createElement('p'); status.className = `pairing-state${state === 'complete' ? ' success' : ''}`;
    status.textContent = state === 'complete' ? 'Pairing complete.' : 'Approved. Finishing the secure connection…'; card.append(status);
  }
  return card;
}

async function refresh() {
  try {
    const pending = await invoke('relay_pending');
    for (const item of pending) known.set(item.id, item);
    const states = new Map(pending.map(item => [item.id, item.status]));
    for (const item of pending) if (item.status === 'finishing') finishing.add(item.id);
    for (const id of finishing) {
      const progress = await invoke('relay_pairing_progress', { requestId: id });
      if (!progress || progress.status === 'rejected') { finishing.delete(id); continue; }
      states.set(id, progress.status);
    }
    const nextRenderedState = JSON.stringify([...states].map(([id, state]) => {
      const item = known.get(id);
      return [id, state, item?.name, item?.origin, item?.server_url, item?.roots];
    }));
    if (nextRenderedState === renderedState) return;
    const cards = [...states].map(([id, state]) => requestCard(known.get(id), state));
    requestsNode.replaceChildren(...cards);
    renderedState = nextRenderedState;
    const active = [...states.values()].some(state => state === 'pending' || state === 'finishing');
    const completed = [...states.values()].some(state => state === 'complete');
    if (!cards.length) requestsNode.innerHTML = '<p class="muted">The request was cancelled or expired.</p>';
    if (completed && !active) setTimeout(closeWindow, 900);
  } catch (error) { fail(error); }
}

refresh();
setInterval(refresh, 750);
