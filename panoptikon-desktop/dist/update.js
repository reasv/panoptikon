const tauri = window.__TAURI__;
const invoke = tauri?.core?.invoke;
const byId = (id) => document.getElementById(id);
let state = null;
let downloaded = 0;
let installing = false;

const previewState = {
  presentation_state: 'available',
  current_version: '0.1.4', available: true, target_version: '0.3.0',
  published_at: '2026-08-01', checking: false, fresh: true, can_install: true,
  check_automatically: true, ribbon_visible: true, updates_disabled: false,
  active_work: true, active_work_unknown: false, last_success_unix: Math.floor(Date.now() / 1000) - 420,
  releases: [
    { version: '0.3.0', date: '2026-08-01', notes_markdown: '### Highlights\n\nA much better Desktop update experience.\n\n### Added\n\n- A dedicated update window with release notes.\n- Gentle reminders and persistent update awareness.\n\n### Fixed\n\n- Update checks no longer delay startup.' },
    { version: '0.2.0', date: '2026-07-10', notes_markdown: '### Added\n\n- Faster local search startup.\n- Improved first-run guidance.' }
  ]
};

function setHidden(id, hidden) { byId(id).hidden = hidden; }
function text(id, value) { byId(id).textContent = value ?? ''; }
function formatVersion(value) { return value ? `Version ${value}` : 'Unknown version'; }
function formatPublishedAt(value) {
  if (!value) return '';
  const dateOnly = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  const date = dateOnly
    ? new Date(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3]))
    : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return `Published ${new Intl.DateTimeFormat(undefined, { dateStyle: 'long' }).format(date)}`;
}

function renderInline(container, source) {
  const pattern = /(\[([^\]]+)\]\((https:\/\/[^)]+)\)|`([^`]+)`|\*\*([^*]+)\*\*)/g;
  let offset = 0; let match;
  while ((match = pattern.exec(source))) {
    container.append(document.createTextNode(source.slice(offset, match.index)));
    if (match[2]) {
      const url = match[3];
      const link = document.createElement('a'); link.href = url; link.textContent = match[2];
      link.addEventListener('click', (event) => { event.preventDefault(); invoke?.('open_update_link', { url }); });
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
  const workMayBeActive = Boolean(next.active_work || next.active_work_unknown);
  const nextWorkState = next.active_work_unknown ? 'unknown' : (next.active_work ? 'active' : 'idle');
  const previousWorkState = state
    ? (state.active_work_unknown ? 'unknown' : (state.active_work ? 'active' : 'idle'))
    : null;
  if (state && (state.target_version !== next.target_version || previousWorkState !== nextWorkState)) {
    byId('confirm-active-work').checked = false;
  }
  state = next;
  const available = Boolean(next.available && next.target_version);
  const presentation = next.presentation_state || (available ? 'available' : 'unchecked');
  const surfaceAvailable = available && presentation !== 'disabled';
  const titles = {
    available: 'Panoptikon update available',
    checking: 'Checking for updates',
    failed: 'Unable to check for updates',
    current: 'Panoptikon is up to date',
    unchecked: 'Update status not checked',
    disabled: 'Updates disabled',
  };
  text('title', titles[presentation] || titles.unchecked);
  text('versions', surfaceAvailable
    ? `Installed ${next.current_version}  →  Available ${next.target_version}`
    : `Panoptikon Desktop ${next.current_version}`);
  const published = surfaceAvailable ? formatPublishedAt(next.published_at) : '';
  text('published', published);
  setHidden('published', !published);
  setHidden('checking', !next.checking);
  setHidden('active-work', !(surfaceAvailable && workMayBeActive && !installing));
  text('active-work-title', next.active_work_unknown ? 'Panoptikon activity could not be verified' : 'Panoptikon is processing files');
  text('active-work-description', next.active_work_unknown
    ? 'Desktop could not confirm whether Panoptikon is processing files. It will not stop Panoptikon unless a final safety check succeeds.'
    : 'Installing now will stop the current task. Incomplete work can be retried after Panoptikon restarts.');
  text('confirm-active-work-label', next.active_work_unknown
    ? 'Retry the safety check before installing'
    : 'I understand and want to install now');
  setHidden('notes', !surfaceAvailable || installing);
  const showEmpty = ['current', 'unchecked', 'disabled'].includes(presentation) && !installing;
  setHidden('empty', !showEmpty);
  const showError = (presentation === 'failed' || (surfaceAvailable && next.last_error)) && !next.checking && !installing;
  setHidden('error', !showError);
  const successfulCheck = next.last_success_unix
    ? ` Last successful check: ${new Date(next.last_success_unix * 1000).toLocaleString()}.`
    : '';
  text('error-text', next.last_error
    ? `${next.last_error}.${surfaceAvailable ? ' Cached update information is still shown.' : successfulCheck}`
    : 'The update service could not be reached.');
  const emptyCopy = {
    current: ['Panoptikon is up to date', 'You already have the newest available version.'],
    unchecked: ['Update status not checked', 'Check for updates to see whether a newer version is available.'],
    disabled: ['Updates disabled', 'Update checks are disabled in development builds.'],
  };
  const [emptyTitle, emptyDescription] = emptyCopy[presentation] || emptyCopy.unchecked;
  text('empty-title', emptyTitle);
  text('empty-description', emptyDescription);
  const notes = byId('notes'); notes.replaceChildren();
  (next.releases || []).forEach((release, index) => {
    const section = document.createElement('section'); section.className = 'release';
    const heading = document.createElement('div'); heading.className = 'release-header';
    const h2 = document.createElement('h2'); h2.textContent = formatVersion(release.version); heading.append(h2);
    if (release.date) { const date = document.createElement('span'); date.className = 'release-date'; date.textContent = release.date; heading.append(date); }
    if (index === 0) { const badge = document.createElement('span'); badge.className = 'badge'; badge.textContent = 'Latest'; heading.append(badge); }
    section.append(heading, renderMarkdown(release.notes_markdown)); notes.append(section);
  });
  byId('install').disabled = !surfaceAvailable || !next.can_install || installing || (workMayBeActive && !byId('confirm-active-work').checked);
  byId('reminder').disabled = !surfaceAvailable || installing;
  byId('later').disabled = installing;
}

async function refresh({ checkIfStale = false } = {}) {
  if (!invoke) { render(previewState); return; }
  try {
    let next = await invoke('get_update_state'); render(next);
    if (checkIfStale && !next.updates_disabled && next.available && (!next.fresh || !next.can_install) && !next.checking) {
      next = await invoke('check_for_updates'); render(next);
    }
  } catch (error) {
    setHidden('error', false); text('error-text', String(error));
  }
}

byId('retry').addEventListener('click', async () => {
  try { render({ ...state, checking: true, presentation_state: state?.available ? 'available' : 'checking' }); render(await invoke('check_for_updates')); }
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
      confirmedWorkState: byId('confirm-active-work').checked
        ? (state.active_work_unknown ? 'unknown' : (state.active_work ? 'active' : null))
        : null,
    });
  } catch (error) {
    installing = false;
    if (String(error).includes('TARGET_CHANGED')) {
      await refresh(); text('message', 'A newer update was found. Review its notes before installing.'); setHidden('message', false);
    } else if (String(error).includes('ACTIVE_WORK_UNKNOWN')) {
      await refresh(); text('message', 'Desktop could not confirm that Panoptikon is idle. Panoptikon was left running; try again.'); setHidden('message', false);
    } else if (String(error).includes('ACTIVE_WORK')) {
      await refresh(); text('message', 'Panoptikon started processing files. Review the warning before installing.'); setHidden('message', false);
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
