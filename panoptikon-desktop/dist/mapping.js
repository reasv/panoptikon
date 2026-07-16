const invoke = window.__TAURI__?.core?.invoke ?? (async () => { throw new Error('Desktop bridge is unavailable.'); });
const actionsNode = document.getElementById('actions');
const errorNode = document.getElementById('error');
const drafts = new Map();
let closing = false;

function fail(error) { errorNode.hidden = false; errorNode.textContent = String(error); }
async function closeWindow() { if (!closing) { closing = true; await invoke('close_relay_mapping_window'); } }

function actionCard(action) {
  const draft = drafts.get(action.id) ?? { remote: action.suggested_remote_root, local: '' }; drafts.set(action.id, draft);
  const card = document.createElement('article'); card.className = 'mapping-request';
  const title = document.createElement('strong'); title.textContent = action.action === 'open_file' ? 'Open File is waiting' : 'Show in Folder is waiting';
  const path = document.createElement('code'); path.textContent = action.remote_path;
  const fields = document.createElement('div'); fields.className = 'mapping-fields';
  const remoteId = `remote-${action.id}`; const localId = `local-${action.id}`;
  const remoteLabel = document.createElement('label'); remoteLabel.htmlFor = remoteId; remoteLabel.textContent = 'Server folder prefix';
  const remote = document.createElement('input'); remote.id = remoteId; remote.value = draft.remote; remote.oninput = () => { draft.remote = remote.value; };
  const localLabel = document.createElement('label'); localLabel.htmlFor = localId; localLabel.textContent = 'Matching folder on this computer';
  const local = document.createElement('input'); local.id = localId; local.value = draft.local; local.placeholder = 'Choose a folder'; local.oninput = () => { draft.local = local.value; };
  const choose = document.createElement('button'); choose.textContent = 'Choose folder…'; choose.onclick = async () => { const folder = await invoke('choose_relay_mapping_folder'); if (folder) { local.value = folder; draft.local = folder; } };
  fields.append(remoteLabel, remote, localLabel, local, choose);
  const preview = document.createElement('p'); preview.className = 'mapping-preview';
  const buttons = document.createElement('div'); buttons.className = 'mapping-actions';
  const check = document.createElement('button'); check.textContent = 'Preview'; check.onclick = async () => { try { const result = await invoke('relay_mapping_preview', { actionId: action.id, remote: remote.value.trim(), local: local.value.trim() }); preview.textContent = `${result.translated_path} — ${result.exists ? 'file exists' : 'file does not exist'}`; preview.className = result.exists ? 'mapping-preview success' : 'mapping-preview inline-error'; } catch (error) { fail(error); } };
  const save = document.createElement('button'); save.className = 'primary'; save.textContent = 'Save mapping and continue'; save.onclick = async () => { try { save.disabled = true; await invoke('relay_resolve_mapping', { actionId: action.id, remote: remote.value.trim(), local: local.value.trim() }); drafts.delete(action.id); await refresh(); } catch (error) { save.disabled = false; fail(error); } };
  buttons.append(check, save); card.append(title, path, fields, preview, buttons); return card;
}

async function refresh() {
  try {
    const actions = await invoke('relay_mapping_pending');
    actionsNode.replaceChildren(...actions.map(actionCard));
    if (!actions.length) { actionsNode.innerHTML = '<p class="success">Mapping saved. Continuing the file action…</p>'; setTimeout(closeWindow, 700); }
  } catch (error) { fail(error); }
}

refresh();
setInterval(refresh, 1000);
