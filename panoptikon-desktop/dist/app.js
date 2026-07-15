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
  const messages = {
    available: `Panoptikon Desktop ${update.target_version} is available. You have ${update.current_version}.`,
    checking: 'Checking for updates…',
    failed: 'Unable to check for updates.',
    current: `Panoptikon Desktop ${update.current_version} is up to date.`,
    unchecked: 'Panoptikon has not checked for updates yet.',
    disabled: 'Update checks are disabled in development builds.',
  };
  byId('update-version').textContent = messages[update.presentation_state] || messages.unchecked;
  byId('update-last-check').textContent = update.last_success_unix
    ? `Last checked successfully ${relativeTime(update.last_success_unix)}.`
    : 'Panoptikon has not completed an update check yet.';
  if (update.last_error && (update.presentation_state === 'failed' || !update.last_success_unix || update.last_error_unix > update.last_success_unix)) {
    byId('update-error').hidden = false;
    byId('update-error').textContent = `Last attempt failed ${relativeTime(update.last_error_unix)}: ${update.last_error}.`;
  } else byId('update-error').hidden = true;
  byId('automatic-updates').checked = update.check_automatically;
  byId('automatic-updates').disabled = update.updates_disabled;
  byId('check-updates').disabled = update.updates_disabled;
  byId('view-update').hidden = !update.available || update.updates_disabled;
}
const samplePath = navigator.userAgent.includes('Windows') ? 'C:\\Media\\example.jpg' : '/home/user/Media/example.jpg';
function insertPlaceholder(field, value) {
  const start = field.selectionStart ?? field.value.length; const end = field.selectionEnd ?? start;
  field.value = field.value.slice(0, start) + value + field.value.slice(end); field.focus(); field.setSelectionRange(start + value.length, start + value.length);
  field.dispatchEvent(new Event('input', { bubbles: true }));
}
function addArgument(list, value = '') {
  const row = document.createElement('div'); row.className = 'argument-row';
  const input = document.createElement('input'); input.value = value; input.placeholder = 'Argument';
  const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = 'Remove'; remove.onclick = () => { row.remove(); list.closest('fieldset').dispatchEvent(new Event('input', { bubbles: true })); };
  row.append(input, remove); list.append(row); return input;
}
function commandPreview(box) {
  const mode = box.querySelector('[data-field=mode]').value;
  if (mode === 'system_default') return 'System default';
  const path = box.querySelector('[data-field=test_path]').value || samplePath;
  const folder = path.replace(/[\\/][^\\/]*$/, '') || path; const filename = path.split(/[\\/]/).pop() || '';
  const expand = value => value.replaceAll('{path}', path).replaceAll('{folder}', folder).replaceAll('{filename}', filename);
  if (mode === 'custom_shell') return expand(box.querySelector('[data-field=shell_command]').value);
  const program = expand(box.querySelector('[data-field=program]').value);
  const args = [...box.querySelectorAll('.argument-row input')].map(input => `"${expand(input.value)}"`);
  return [`"${program}"`, ...args].join(' ');
}
function updateCommandCard(box) {
  const mode = box.querySelector('[data-field=mode]').value;
  if (mode === 'specific_application' && !box.querySelector('.argument-row')) addArgument(box.querySelector('[data-field=args]'), '{path}');
  box.querySelector('[data-panel=direct]').hidden = !['specific_application', 'custom_direct'].includes(mode);
  box.querySelector('[data-panel=shell]').hidden = mode !== 'custom_shell';
  box.querySelector('[data-role=preview]').textContent = commandPreview(box);
}
function commandEditor(container, commands) {
  container.replaceChildren();
  for (const [key, label] of [['open_file', 'Open File'], ['reveal_in_folder', 'Show in Folder']]) {
    const spec = commands[key] || { mode: 'system_default', program: '', args: [], shell_command: '' };
    const box = document.createElement('fieldset'); box.dataset.command = key; box.className = 'command-card';
    const legend = document.createElement('legend'); legend.textContent = label;
    const methodLabel = document.createElement('label'); methodLabel.textContent = 'Method';
    const mode = document.createElement('select'); mode.dataset.field = 'mode';
    for (const [value, text] of [['system_default','System default'], ['specific_application','Specific application'], ['custom_direct','Custom direct command'], ['custom_shell','Custom shell command']]) { const option = document.createElement('option'); option.value = value; option.textContent = text; mode.append(option); }
    mode.value = spec.mode || (spec.shell_command ? 'custom_shell' : spec.program ? 'custom_direct' : 'system_default'); methodLabel.append(mode);

    const direct = document.createElement('div'); direct.dataset.panel = 'direct'; direct.className = 'command-panel';
    const programLabel = document.createElement('label'); programLabel.textContent = 'Application or executable';
    const programRow = document.createElement('div'); programRow.className = 'picker-row';
    const program = document.createElement('input'); program.dataset.field = 'program'; program.value = spec.program || ''; program.placeholder = 'Choose or enter an executable';
    const chooseProgram = document.createElement('button'); chooseProgram.type = 'button'; chooseProgram.textContent = 'Choose application…'; chooseProgram.onclick = async () => { const value = await invoke('choose_file_action_application'); if (value) { program.value = value; updateCommandCard(box); } };
    programRow.append(program, chooseProgram); programLabel.append(programRow); direct.append(programLabel);
    const argsTitle = document.createElement('span'); argsTitle.textContent = 'Arguments'; direct.append(argsTitle);
    const args = document.createElement('div'); args.dataset.field = 'args'; for (const arg of spec.args || []) addArgument(args, arg); direct.append(args);
    const addArg = document.createElement('button'); addArg.type = 'button'; addArg.textContent = '+ Add argument'; addArg.onclick = () => addArgument(args).focus(); direct.append(addArg);

    const shellPanel = document.createElement('div'); shellPanel.dataset.panel = 'shell'; shellPanel.className = 'command-panel';
    const warning = document.createElement('p'); warning.className = 'warning'; warning.textContent = 'Shell commands can execute arbitrary programs and scripts. Paths are inserted exactly where placeholders appear.';
    const shell = document.createElement('textarea'); shell.rows = 4; shell.dataset.field = 'shell_command'; shell.value = spec.shell_command || ''; shell.placeholder = 'Shell command'; shellPanel.append(warning, shell);

    const placeholders = document.createElement('div'); placeholders.className = 'placeholder-row'; placeholders.append('Insert placeholder: ');
    for (const value of ['{path}', '{folder}', '{filename}']) { const button = document.createElement('button'); button.type = 'button'; button.textContent = value; button.onclick = () => { const focused = box.querySelector('input:focus, textarea:focus') || (mode.value === 'custom_shell' ? shell : addArgument(args)); insertPlaceholder(focused, value); }; placeholders.append(button); }
    const previewLabel = document.createElement('strong'); previewLabel.textContent = 'Expanded preview';
    const preview = document.createElement('code'); preview.dataset.role = 'preview'; preview.className = 'command-preview';
    const testRow = document.createElement('div'); testRow.className = 'test-row';
    const testPath = document.createElement('input'); testPath.dataset.field = 'test_path'; testPath.placeholder = 'Choose a local test file';
    const chooseTest = document.createElement('button'); chooseTest.type = 'button'; chooseTest.textContent = 'Choose test file…'; chooseTest.onclick = async () => { const value = await invoke('choose_file_action_test_file'); if (value) { testPath.value = value; updateCommandCard(box); } };
    const runTest = document.createElement('button'); runTest.type = 'button'; runTest.textContent = 'Test action'; runTest.onclick = async () => { try { if (!testPath.value) throw new Error('Choose a test file first.'); const result = await invoke('test_file_action', { command: readCommand(box), path: testPath.value }); status.textContent = `${result.message}. Preview: ${result.preview}`; status.className = result.exit_code && result.exit_code !== 0 ? 'inline-error' : 'success'; } catch (error) { status.textContent = String(error); status.className = 'inline-error'; } };
    testRow.append(testPath, chooseTest, runTest);
    const status = document.createElement('p'); status.setAttribute('aria-live', 'polite'); status.className = 'muted';
    const reset = document.createElement('button'); reset.type = 'button'; reset.textContent = 'Reset this action to system default'; reset.onclick = () => { mode.value = 'system_default'; program.value = ''; args.replaceChildren(); shell.value = ''; updateCommandCard(box); };
    box.append(legend, methodLabel, direct, shellPanel, placeholders, previewLabel, preview, testRow, status, reset);
    box.addEventListener('input', () => updateCommandCard(box)); mode.addEventListener('change', () => updateCommandCard(box)); updateCommandCard(box); container.append(box);
  }
}
function readCommand(box) {
  const mode = box.querySelector('[data-field=mode]').value;
  const program = ['specific_application', 'custom_direct'].includes(mode) ? box.querySelector('[data-field=program]').value.trim() : '';
  const shell_command = mode === 'custom_shell' ? box.querySelector('[data-field=shell_command]').value.trim() : '';
  const args = ['specific_application', 'custom_direct'].includes(mode) ? [...box.querySelectorAll('.argument-row input')].map(input => input.value) : [];
  if (mode !== 'system_default' && !program && !shell_command) throw new Error('Choose an application or enter a command.');
  if (mode !== 'system_default' && ![program, shell_command, ...args].some(value => /\{(?:path|folder|filename)\}/.test(value))) throw new Error('Each custom action must use at least one path placeholder.');
  return { mode, program, shell_command, args };
}
function readCommands(container) { const result = {}; for (const box of container.querySelectorAll('[data-command]')) result[box.dataset.command] = readCommand(box); return result; }
function mappingEditor(item) {
  const card = document.createElement('article'); card.dataset.instance = item.id;
  const title = document.createElement('strong'); title.textContent = item.name;
  const detail = document.createElement('p'); detail.textContent = `${item.server_url} — ${item.origins.join(', ')}`;
  card.append(title, detail);
  for (const mapping of item.mappings) {
    const row = document.createElement('div'); row.className = 'mapping-row';
    const remote = document.createElement('input'); remote.value = mapping.remote; remote.placeholder = 'Server folder';
    const local = document.createElement('input'); local.value = mapping.local; local.placeholder = 'Choose the matching folder, or leave blank to skip';
    const browse = document.createElement('button'); browse.textContent = 'Choose folder…'; browse.onclick = async () => { const folder = await invoke('choose_relay_folder'); if (folder) local.value = folder; };
    const remove = document.createElement('button'); remove.textContent = 'Remove'; remove.onclick = () => row.remove();
    row.append(remote, local, browse, remove); card.append(row);
  }
  const add = document.createElement('button'); add.textContent = '+ Add mapping'; add.onclick = () => { item.mappings.push({ remote: '', local: '' }); const replacement = mappingEditor(item); card.replaceWith(replacement); };
  const save = document.createElement('button'); save.textContent = 'Save mappings'; save.onclick = async () => {
    const mappings = [...card.querySelectorAll('.mapping-row')].map(row => { const inputs = row.querySelectorAll('input'); return { remote: inputs[0].value.trim(), local: inputs[1].value.trim() }; }).filter(mapping => mapping.remote);
    await invoke('relay_set_mappings', { instanceId: item.id, mappings }); await refresh();
  };
  const revoke = document.createElement('button'); revoke.textContent = 'Revoke'; revoke.onclick = async () => { if (confirm(`Revoke Relay access for ${item.name}?`)) { await invoke('relay_revoke', { instanceId: item.id }); await refresh(); } };
  card.append(add, save, revoke); return card;
}
function pendingMappingEditor(action) {
  const card = document.createElement('article'); card.className = 'pending-mapping';
  const title = document.createElement('strong'); title.textContent = action.action === 'open_file' ? 'Open File needs a folder mapping' : 'Show in Folder needs a folder mapping';
  const exact = document.createElement('code'); exact.className = 'unmatched-path'; exact.textContent = action.remote_path;
  const remote = document.createElement('input'); remote.value = action.suggested_remote_root; remote.placeholder = 'Server folder prefix';
  const localRow = document.createElement('div'); localRow.className = 'picker-row'; const local = document.createElement('input'); local.placeholder = 'Matching local folder';
  const choose = document.createElement('button'); choose.textContent = 'Choose folder…'; choose.onclick = async () => { const value = await invoke('choose_relay_folder'); if (value) local.value = value; }; localRow.append(local, choose);
  const preview = document.createElement('p'); preview.className = 'muted'; preview.setAttribute('aria-live', 'polite');
  const check = document.createElement('button'); check.textContent = 'Preview translation'; check.onclick = async () => { try { const result = await invoke('relay_mapping_preview', { actionId: action.id, remote: remote.value.trim(), local: local.value.trim() }); preview.textContent = `${result.translated_path} — ${result.exists ? 'file exists' : 'file does not exist'}`; preview.className = result.exists ? 'success' : 'inline-error'; } catch (error) { preview.textContent = String(error); preview.className = 'inline-error'; } };
  const save = document.createElement('button'); save.className = 'primary'; save.textContent = 'Save mapping and continue'; save.onclick = async () => { try { const result = await invoke('relay_mapping_preview', { actionId: action.id, remote: remote.value.trim(), local: local.value.trim() }); if (!result.exists) throw new Error('The translated file does not exist.'); await invoke('relay_resolve_mapping', { actionId: action.id, remote: remote.value.trim(), local: local.value.trim() }); await refresh(); } catch (error) { fail(error); } };
  card.append(title, document.createTextNode('Panoptikon requested:'), exact, remote, localRow, preview, check, save); return card;
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
    if (!byId('file-commands').children.length) commandEditor(byId('file-commands'), await invoke('file_action_commands'));
    byId('relay').replaceChildren(...pending.map((item) => {
      const row = document.createElement('p');
      row.textContent = `${item.name} — ${item.origin}. Suggested roots: ${item.roots.length ? item.roots.join(', ') : 'none'} `;
      const approve = document.createElement('button'); approve.textContent = 'Approve'; approve.onclick = () => invoke('relay_approve', { requestId: item.id }).then(refresh).catch(fail);
      const reject = document.createElement('button'); reject.textContent = 'Reject'; reject.onclick = () => invoke('relay_reject', { requestId: item.id }).then(refresh).catch(fail);
      row.append(approve, reject); return row;
    }));
    if (!byId('relay-instances').contains(document.activeElement)) byId('relay-instances').replaceChildren(...relayStatus.instances.map(mappingEditor));
    if (!byId('pending-mappings').contains(document.activeElement)) byId('pending-mappings').replaceChildren(...relayStatus.pending_actions.map(pendingMappingEditor));
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
      if (button.disabled) return;
      button.disabled = true; const original = button.textContent; button.textContent = 'Checking…';
      try {
        const update = await invoke('check_for_updates'); showUpdate(update);
        if (update.available) await invoke('open_update_window');
        else if (update.presentation_state === 'current') alert(`Panoptikon Desktop ${update.current_version} is up to date.`);
      }
      catch (error) {
        const update = await invoke('get_update_state'); showUpdate(update);
        if (update.presentation_state !== 'failed') throw error;
      }
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
byId('save-file-commands').addEventListener('click', async () => { try { await invoke('set_file_action_commands', { commands: readCommands(byId('file-commands')) }); byId('file-command-status').textContent = 'File-opening settings saved.'; } catch (error) { fail(error); } });
window.__TAURI__.event.listen('desktop-state', refresh);
window.__TAURI__.event.listen('desktop-update-state', (event) => showUpdate(event.payload));
refresh();
setInterval(refresh, 3000);
