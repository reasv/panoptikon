const invoke = window.__TAURI__.core.invoke;
const byId = (id) => document.getElementById(id);
function fail(error) { const box = byId('error'); box.hidden = false; box.textContent = String(error); }
function relativeTime(unix) {
  if (!unix) return 'Never';
  const seconds = Math.max(0, Math.floor(Date.now() / 1000) - unix);
  const relative = seconds < 60 ? 'just now' : seconds < 3600 ? `${Math.floor(seconds / 60)} minutes ago` : seconds < 86400 ? `${Math.floor(seconds / 3600)} hours ago` : `${Math.floor(seconds / 86400)} days ago`;
  return `${relative} (${new Date(unix * 1000).toLocaleString()})`;
}
function showUpdate(update) {
  byId('update-version').textContent = update.available
    ? `Panoptikon Desktop ${update.target_version} is available. You have ${update.current_version}.`
    : `Panoptikon Desktop ${update.current_version} is up to date.`;
  byId('update-last-check').textContent = update.last_success_unix
    ? `Last checked successfully ${relativeTime(update.last_success_unix)}.`
    : 'Panoptikon has not completed an update check yet.';
  if (update.last_error && (!update.last_success_unix || update.last_error_unix > update.last_success_unix)) {
    byId('update-error').hidden = false;
    byId('update-error').textContent = `Last attempt failed ${relativeTime(update.last_error_unix)}: ${update.last_error}.`;
  } else byId('update-error').hidden = true;
  byId('automatic-updates').checked = update.check_automatically;
  byId('view-update').hidden = !update.available;
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
    showUpdate(await invoke('get_update_state'));
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
    if (button.dataset.action === 'updates') {
      button.disabled = true; const original = button.textContent; button.textContent = 'Checking…';
      try { const update = await invoke('check_for_updates'); showUpdate(update); if (update.available) await invoke('open_update_window'); else alert(`Panoptikon Desktop ${update.current_version} is up to date.`); }
      finally { button.disabled = false; button.textContent = original; }
    }
    if (button.dataset.action === 'refresh') await refresh();
    if (button.dataset.action === 'quit' && confirm('Quit Panoptikon Desktop and stop the local Server?')) await invoke('quit_desktop');
  } catch (error) { fail(error); }
});
byId('view-update').addEventListener('click', () => invoke('open_update_window').catch(fail));
byId('automatic-updates').addEventListener('change', async (event) => {
  try { showUpdate(await invoke('set_automatic_update_checks', { enabled: event.target.checked })); }
  catch (error) { event.target.checked = !event.target.checked; fail(error); }
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
window.__TAURI__.event.listen('desktop-update-state', (event) => showUpdate(event.payload));
refresh();
setInterval(refresh, 3000);
