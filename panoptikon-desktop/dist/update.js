const tauri = window.__TAURI__;
const invoke = tauri?.core?.invoke;
const byId = (id) => document.getElementById(id);
let state = null;
let downloaded = 0;
let installing = false;

const previewState = {
  current_version: '0.1.4', available: true, target_version: '0.3.0',
  published_at: '2026-08-01', checking: false, fresh: true, can_install: true,
  check_automatically: true, ribbon_visible: true, updates_disabled: false,
  active_work: true, last_success_unix: Math.floor(Date.now() / 1000) - 420,
  releases: [
    { version: '0.3.0', date: '2026-08-01', notes_markdown: '### Highlights\n\nA much better Desktop update experience.\n\n### Added\n\n- A dedicated update window with release notes.\n- Gentle reminders and persistent update awareness.\n\n### Fixed\n\n- Update checks no longer delay startup.' },
    { version: '0.2.0', date: '2026-07-10', notes_markdown: '### Added\n\n- Faster local search startup.\n- Improved first-run guidance.' }
  ]
};

function setHidden(id, hidden) { byId(id).hidden = hidden; }
function text(id, value) { byId(id).textContent = value ?? ''; }
function formatVersion(value) { return value ? `Version ${value}` : 'Unknown version'; }

function renderInline(container, source) {
  const pattern = /(\[([^\]]+)\]\((https:\/\/[^)]+)\)|`([^`]+)`|\*\*([^*]+)\*\*)/g;
  let offset = 0; let match;
  while ((match = pattern.exec(source))) {
    container.append(document.createTextNode(source.slice(offset, match.index)));
    if (match[2]) {
      const link = document.createElement('a'); link.href = match[3]; link.textContent = match[2];
      link.addEventListener('click', (event) => { event.preventDefault(); invoke?.('open_update_link', { url: match[3] }); });
      container.append(link);
    } else if (match[4]) {
      const code = document.createElement('code'); code.textContent = match[4]; container.append(code);
    } else {
      const strong = document.createElement('strong'); strong.textContent = match[5]; container.append(strong);
    }
    offset = pattern.lastIndex;
  }
  container.append(document.createTextNode(source.slice(offset)));
}

function renderMarkdown(markdown) {
  const root = document.createElement('div'); root.className = 'markdown';
  let list = null;
  const appendText = (tag, value) => { const el = document.createElement(tag); renderInline(el, value); root.append(el); };
  for (const raw of String(markdown || '').replace(/\r/g, '').split('\n')) {
    const line = raw.trim();
    if (!line) { list = null; continue; }
    if (line.startsWith('### ')) { list = null; appendText('h3', line.slice(4)); continue; }
    if (line.startsWith('- ')) {
      if (!list) { list = document.createElement('ul'); root.append(list); }
      const item = document.createElement('li'); renderInline(item, line.slice(2)); list.append(item); continue;
    }
    list = null; appendText('p', line);
  }
  return root;
}

function render(next) {
  state = next;
  const available = Boolean(next.available && next.target_version);
  text('title', available ? 'Update available' : (next.checking ? 'Checking for updates' : 'Panoptikon is up to date'));
  text('versions', available
    ? `Installed ${next.current_version}  →  Available ${next.target_version}`
    : `Panoptikon Desktop ${next.current_version}`);
  setHidden('checking', !next.checking);
  setHidden('active-work', !(available && next.active_work && !installing));
  setHidden('notes', !available || installing);
  setHidden('empty', available || next.checking || installing);
  setHidden('error', !next.last_error || next.checking || installing);
  text('error-text', next.last_error ? `${next.last_error}. Cached update information is still shown when available.` : '');
  text('empty-description', next.updates_disabled
    ? 'Update checks are disabled in development builds.'
    : 'You already have the newest available version.');
  const notes = byId('notes'); notes.replaceChildren();
  (next.releases || []).forEach((release, index) => {
    const section = document.createElement('section'); section.className = 'release';
    const heading = document.createElement('div'); heading.className = 'release-header';
    const h2 = document.createElement('h2'); h2.textContent = formatVersion(release.version); heading.append(h2);
    if (release.date) { const date = document.createElement('span'); date.className = 'release-date'; date.textContent = release.date; heading.append(date); }
    if (index === 0) { const badge = document.createElement('span'); badge.className = 'badge'; badge.textContent = 'Latest'; heading.append(badge); }
    section.append(heading, renderMarkdown(release.notes_markdown)); notes.append(section);
  });
  byId('install').disabled = !next.can_install || installing || (next.active_work && !byId('confirm-active-work').checked);
  byId('reminder').disabled = !available || installing;
  byId('later').disabled = installing;
}

async function refresh({ checkIfStale = false } = {}) {
  if (!invoke) { render(previewState); return; }
  try {
    let next = await invoke('get_update_state'); render(next);
    if (checkIfStale && next.available && !next.fresh && !next.checking) {
      next = await invoke('check_for_updates'); render(next);
    }
  } catch (error) {
    setHidden('error', false); text('error-text', String(error));
  }
}

byId('retry').addEventListener('click', async () => {
  try { render({ ...state, checking: true }); render(await invoke('check_for_updates')); }
  catch (error) { await refresh(); text('error-text', String(error)); setHidden('error', false); }
});
byId('confirm-active-work').addEventListener('change', () => render(state));
byId('later').addEventListener('click', () => invoke ? invoke('close_update_window') : history.back());
byId('reminder').addEventListener('change', async (event) => {
  if (!event.target.value) return;
  try { await invoke('schedule_update_reminder', { preset: event.target.value }); await invoke('close_update_window'); }
  catch (error) { text('error-text', String(error)); setHidden('error', false); }
});
byId('install').addEventListener('click', async () => {
  if (!state?.target_version) return;
  installing = true; downloaded = 0; setHidden('progress', false); setHidden('notes', true); setHidden('active-work', true); render(state);
  try {
    await invoke('install_update', {
      expectedVersion: state.target_version,
      confirmActiveWork: !state.active_work || byId('confirm-active-work').checked,
    });
  } catch (error) {
    installing = false;
    if (String(error).includes('TARGET_CHANGED')) {
      await refresh(); text('message', 'A newer update was found. Review its notes before installing.'); setHidden('message', false);
    } else {
      await refresh(); text('error-text', String(error)); setHidden('error', false);
    }
    setHidden('progress', true);
  }
});

tauri?.event?.listen('desktop-update-state', (event) => render(event.payload));
tauri?.event?.listen('desktop-update-progress', (event) => {
  const { stage, chunk = 0, total, finished } = event.payload;
  const labels = { downloading: 'Downloading update…', verifying: 'Verifying signed update…', stopping: 'Stopping Panoptikon…', installing: 'Installing update…', restarting: 'Restarting Panoptikon…' };
  text('progress-stage', labels[stage] || 'Preparing update…');
  downloaded += chunk;
  const bar = byId('progress-bar');
  if (stage === 'downloading' && total) { bar.max = total; bar.value = downloaded; text('progress-size', `${(downloaded / 1048576).toFixed(1)} of ${(total / 1048576).toFixed(1)} MB`); }
  else { bar.removeAttribute('value'); text('progress-size', ''); }
  if (finished && stage === 'verifying') text('progress-detail', 'The signed download is ready. Panoptikon will now prepare to restart.');
});

refresh({ checkIfStale: true });
