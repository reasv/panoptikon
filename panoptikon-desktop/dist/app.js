const invoke = window.__TAURI__.core.invoke;
const byId = (id) => document.getElementById(id);
function fail(error) { const box = byId('error'); box.hidden = false; box.textContent = String(error); }
function showUpdate(update) {
  byId('update').hidden = false;
  byId('update-message').textContent = `Panoptikon Desktop ${update.version} is available (installed: ${update.current_version}).`;
}
async function refresh() {
  try {
    const status = await invoke('get_status');
    const warnings = await invoke('get_startup_warnings');
    if (warnings.length) {
      const box = byId('error'); box.hidden = false;
      box.textContent = `Recovered from invalid settings:\n${warnings.join('\n')}`;
    }
    byId('state').textContent = status.state_label;
    byId('root').textContent = status.server_root;
    byId('port').textContent = status.port;
    byId('local').checked = status.local_server_enabled;
    const databaseReady = status.default_database_ready === true;
    byId('setup-title').textContent = databaseReady ? 'New Database' : 'Set Up Panoptikon';
    byId('setup-description').textContent = databaseReady
      ? 'Create a separate index database with its own folders, models, settings, and indexed data.'
      : 'Choose the folders and indexing options for your first database.';
    byId('setup-button').textContent = databaseReady ? 'Create New Database' : 'Continue Setup';
    byId('logs').textContent = (await invoke('log_tail', { lines: 150 })).join('\n') || 'No log entries yet.';
    const pending = await invoke('relay_pending');
    const relayStatus = await invoke('relay_status');
    byId('relay-enabled').checked = relayStatus.enabled;
    byId('relay').replaceChildren(...pending.map((item) => {
      const row = document.createElement('p');
      row.textContent = `${item.name} — ${item.origin} `;
      const approve = document.createElement('button'); approve.textContent = 'Approve'; approve.onclick = () => invoke('relay_approve', { requestId: item.id }).then(refresh).catch(fail);
      const reject = document.createElement('button'); reject.textContent = 'Reject'; reject.onclick = () => invoke('relay_reject', { requestId: item.id }).then(refresh).catch(fail);
      row.append(approve, reject); return row;
    }));
    byId('relay-instances').replaceChildren(...relayStatus.instances.map((item) => {
      const card = document.createElement('article');
      const title = document.createElement('strong'); title.textContent = item.name;
      const detail = document.createElement('p'); detail.textContent = `${item.server_url} — ${item.origins.join(', ')}`;
      const mappings = document.createElement('textarea'); mappings.rows = 4;
      mappings.setAttribute('aria-label', `Path mappings for ${item.name}`);
      mappings.value = item.mappings.map((mapping) => `${mapping.remote} => ${mapping.local}`).join('\n');
      const save = document.createElement('button'); save.textContent = 'Save mappings';
      save.onclick = async () => {
        const parsed = mappings.value.split('\n').filter((line) => line.trim()).map((line) => {
          const separator = line.indexOf('=>');
          if (separator < 1) throw new Error('Each mapping must use: remote path => local path');
          return { remote: line.slice(0, separator).trim(), local: line.slice(separator + 2).trim() };
        });
        await invoke('relay_set_mappings', { instanceId: item.id, mappings: parsed }); await refresh();
      };
      const revoke = document.createElement('button'); revoke.textContent = 'Revoke';
      revoke.onclick = async () => { if (confirm(`Revoke Relay access for ${item.name}?`)) { await invoke('relay_revoke', { instanceId: item.id }); await refresh(); } };
      card.append(title, detail, mappings, save, revoke); return card;
    }));
  } catch (error) { fail(error); }
}
document.addEventListener('click', async (event) => {
  const button = event.target.closest('button'); if (!button) return;
  try {
    if (button.dataset.folder) await invoke('open_known_folder', { kind: button.dataset.folder });
    if (button.dataset.action === 'open') await invoke('open_action_command');
    if (button.dataset.action === 'setup') await invoke('open_setup_command');
    if (button.dataset.action === 'restart') await invoke('restart_server');
    if (button.dataset.action === 'updates') { const update = await invoke('check_for_updates'); if (update) showUpdate(update); else alert('No update is available.'); }
    if (button.dataset.action === 'refresh') await refresh();
    if (button.dataset.action === 'quit' && confirm('Quit Panoptikon Desktop and stop the local Server?')) await invoke('quit_desktop');
  } catch (error) { fail(error); }
});
byId('install-update').addEventListener('click', async () => {
  if (!confirm('Download the signed update, stop the local Server, and restart Panoptikon Desktop?')) return;
  try { byId('update-progress').hidden = false; await invoke('install_update'); } catch (error) { fail(error); }
});
byId('local').addEventListener('change', async (event) => {
  const enabled = event.target.checked;
  if (!enabled && !confirm('Stop and disable the local Panoptikon Server? Relay and Desktop will remain available.')) { event.target.checked = true; return; }
  try { await invoke('set_local_server_enabled', { enabled, confirmed: true }); await refresh(); } catch (error) { fail(error); }
});
byId('relay-enabled').addEventListener('change', async (event) => {
  try { await invoke('set_relay_enabled', { enabled: event.target.checked }); await refresh(); } catch (error) { event.target.checked = !event.target.checked; fail(error); }
});
window.__TAURI__.event.listen('desktop-state', refresh);
window.__TAURI__.event.listen('desktop-update-available', (event) => showUpdate(event.payload));
window.__TAURI__.event.listen('desktop-update-progress', (event) => {
  const progress = byId('update-progress'); progress.hidden = false;
  if (event.payload.total) { progress.max = event.payload.total; progress.value += event.payload.chunk || 0; }
});
refresh();
setInterval(refresh, 3000);
