const invoke = window.__TAURI__?.core?.invoke ?? (async () => { throw new Error('Desktop bridge is unavailable.'); });
const byId = (id) => document.getElementById(id);
function selectTab(name) {
  for (const tab of document.querySelectorAll('[role="tab"]')) {
    const selected = tab.dataset.tab === name;
    tab.setAttribute('aria-selected', String(selected));
    tab.tabIndex = selected ? 0 : -1;
  }
  for (const panel of document.querySelectorAll('.tab-panel')) panel.hidden = panel.dataset.panel !== name;
}
for (const tab of document.querySelectorAll('[role="tab"]')) {
  tab.addEventListener('click', () => selectTab(tab.dataset.tab));
  tab.addEventListener('keydown', (event) => {
    if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
    const tabs = [...document.querySelectorAll('[role="tab"]')];
    const next = (tabs.indexOf(tab) + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    selectTab(tabs[next].dataset.tab);
    tabs[next].focus();
  });
}
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
let serverConfiguration = null;
let serverConfigurationDirty = false;
function sourceDescription(field) {
  return field.source.type === 'environment'
    ? `Set by ${field.source.variable} in .env; its TOML environment reference is preserved.`
    : '';
}
function renderSource(id, field) {
  const element = byId(id);
  element.textContent = sourceDescription(field);
  element.hidden = !element.textContent;
}
let lastEditedPort = 'lan-port';
function validatePortConflict(changedPort = lastEditedPort) {
  lastEditedPort = changedPort || lastEditedPort;
  const localPort = Number(byId('local-port').value);
  const lanPort = Number(byId('lan-port').value);
  const conflict = byId('lan-enabled').checked && localPort > 0 && localPort === lanPort;
  for (const id of ['local-port', 'lan-port']) {
    const input = byId(id);
    const error = byId(`${id}-error`);
    input.removeAttribute('aria-invalid');
    input.setCustomValidity('');
    error.hidden = true;
    error.textContent = '';
  }
  if (conflict) {
    const targetId = changedPort === 'local-port' ? 'local-port' : 'lan-port';
    const otherName = targetId === 'local-port' ? 'LAN port' : 'local port';
    const input = byId(targetId);
    const error = byId(`${targetId}-error`);
    const message = `Choose a different port; ${otherName} already uses ${localPort}.`;
    input.setAttribute('aria-invalid', 'true');
    input.setCustomValidity(message);
    error.textContent = message;
    error.hidden = false;
  }
  byId('save-server-configuration').disabled = conflict;
  return !conflict;
}
function updateLanDefaultOptions() {
  if (!serverConfiguration) return;
  const all = byId('lan-all-databases').checked;
  const selected = all
    ? serverConfiguration.databases
    : [...byId('lan-databases').querySelectorAll('input:checked')].map(input => input.value);
  const current = byId('lan-default-database').value || serverConfiguration.lan.default_database;
  const values = selected.length ? selected : [current];
  byId('lan-default-database').replaceChildren(...values.map(value => {
    const option = document.createElement('option'); option.value = value; option.textContent = value; return option;
  }));
  byId('lan-default-database').value = values.includes(current) ? current : values[0];
  byId('lan-databases').disabled = all || serverConfiguration.lan.mode === 'custom';
}
function updateLanVisibility() {
  const custom = serverConfiguration?.lan.mode === 'custom';
  const enabled = byId('lan-enabled').checked;
  byId('lan-fields').hidden = !enabled && !custom;
  for (const field of byId('lan-fields').querySelectorAll('input, select')) field.disabled = custom || (!enabled && field.id !== 'lan-enabled');
  updateLanDefaultOptions();
}
function renderServerConfiguration(configuration) {
  serverConfiguration = configuration;
  byId('local-port').value = configuration.local_port.value;
  renderSource('local-port-source', configuration.local_port);
  byId('prewarm-enabled').checked = configuration.performance.prewarm_enabled.value;
  renderSource('prewarm-enabled-source', configuration.performance.prewarm_enabled);
  byId('prewarm-lazy').checked = configuration.performance.prewarm_lazy.value;
  renderSource('prewarm-lazy-source', configuration.performance.prewarm_lazy);
  byId('loader-concurrency').value = configuration.performance.loader_concurrency.value;
  renderSource('loader-concurrency-source', configuration.performance.loader_concurrency);
  byId('intermediate-budget').value = configuration.performance.intermediate_data_budget_mb.value;
  renderSource('intermediate-budget-source', configuration.performance.intermediate_data_budget_mb);
  byId('embedding-cache-size').value = configuration.performance.embedding_cache_size.value;
  renderSource('embedding-cache-source', configuration.performance.embedding_cache_size);
  const custom = configuration.lan.mode === 'custom';
  byId('lan-enabled').checked = configuration.lan.mode === 'managed';
  byId('lan-enabled').disabled = custom;
  byId('lan-port').value = configuration.lan.port;
  renderSource('lan-port-source', { source: configuration.lan.port_source ?? { type: 'toml' } });
  byId('lan-all-databases').checked = configuration.lan.allowed_databases === null;
  byId('lan-custom').hidden = !custom;
  byId('lan-custom').textContent = custom ? `${configuration.lan.explanation} The simplified controls are read-only; use “Open Config Files” to retain full control.` : '';
  const allowed = new Set(configuration.lan.allowed_databases ?? configuration.databases);
  byId('lan-databases').replaceChildren(...configuration.databases.map(database => {
    const label = document.createElement('label'); const input = document.createElement('input');
    input.type = 'checkbox'; input.value = database; input.checked = allowed.has(database);
    input.addEventListener('change', updateLanDefaultOptions); label.append(input, database); return label;
  }));
  byId('lan-default-database').dataset.selected = configuration.lan.default_database;
  updateLanVisibility();
  validatePortConflict();
  const options = [...byId('lan-default-database').options].map(option => option.value);
  byId('lan-default-database').value = options.includes(configuration.lan.default_database) ? configuration.lan.default_database : options[0] || '';
  serverConfigurationDirty = false;
}
async function loadServerConfiguration(force = false) {
  if (serverConfigurationDirty && !force) return;
  renderServerConfiguration(await invoke('get_server_configuration'));
}
function positiveInteger(id, label, minimum, maximum) {
  const value = Number(byId(id).value);
  if (!Number.isInteger(value) || value < minimum || value > maximum) throw new Error(`${label} must be between ${minimum} and ${maximum}.`);
  return value;
}
function readServerConfiguration() {
  if (!serverConfiguration) throw new Error('Server configuration has not loaded yet.');
  const custom = serverConfiguration.lan.mode === 'custom';
  const lanEnabled = !custom && byId('lan-enabled').checked;
  const allDatabases = byId('lan-all-databases').checked;
  const allowed = allDatabases ? null : [...byId('lan-databases').querySelectorAll('input:checked')].map(input => input.value);
  return {
    revision: serverConfiguration.revision,
    local_port: positiveInteger('local-port', 'Local port', 1, 65535),
    lan: custom ? {
      enabled: false,
      port: serverConfiguration.lan.port,
      allowed_databases: serverConfiguration.lan.allowed_databases,
      default_database: serverConfiguration.lan.default_database,
    } : {
      enabled: lanEnabled,
      port: positiveInteger('lan-port', 'LAN port', 1, 65535),
      allowed_databases: allowed,
      default_database: byId('lan-default-database').value || 'default',
    },
    performance: {
      prewarm_enabled: byId('prewarm-enabled').checked,
      prewarm_lazy: byId('prewarm-lazy').checked,
      loader_concurrency: positiveInteger('loader-concurrency', 'Concurrent file loaders', 1, 256),
      intermediate_data_budget_mb: positiveInteger('intermediate-budget', 'Intermediate-data memory', 64, 1048576),
      embedding_cache_size: positiveInteger('embedding-cache-size', 'Embedding cache size', 0, 65536),
    },
  };
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
    if (!serverConfiguration) await loadServerConfiguration();
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
for (const section of [byId('network-settings'), byId('performance-settings')]) section.addEventListener('input', () => { serverConfigurationDirty = true; byId('server-configuration-status').textContent = ''; });
byId('lan-enabled').addEventListener('change', updateLanVisibility);
byId('lan-all-databases').addEventListener('change', updateLanDefaultOptions);
byId('local-port').addEventListener('input', () => validatePortConflict('local-port'));
byId('lan-port').addEventListener('input', () => validatePortConflict('lan-port'));
byId('lan-enabled').addEventListener('change', () => validatePortConflict('lan-port'));
byId('save-server-configuration').addEventListener('click', async () => {
  const button = byId('save-server-configuration');
  if (!validatePortConflict()) {
    byId(lastEditedPort).focus();
    return;
  }
  try {
    button.disabled = true; button.textContent = 'Saving and restarting…';
    const saved = await invoke('set_server_configuration', { configuration: readServerConfiguration() });
    renderServerConfiguration(saved);
    byId('server-configuration-status').textContent = 'Configuration saved; Server is restarting.';
  } catch (error) { fail(error); }
  finally { button.disabled = !validatePortConflict(); button.textContent = 'Save and restart Server'; }
});
byId('save-file-commands').addEventListener('click', async () => { try { await invoke('set_file_action_commands', { commands: readCommands(byId('file-commands')) }); byId('file-command-status').textContent = 'File-opening settings saved.'; } catch (error) { fail(error); } });
window.__TAURI__?.event?.listen('desktop-state', refresh);
window.__TAURI__?.event?.listen('desktop-update-state', (event) => showUpdate(event.payload));
refresh();
setInterval(refresh, 3000);
