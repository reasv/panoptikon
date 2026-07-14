const byId = (id) => document.getElementById(id);

const copyForState = {
  installing: {
    title: "Preparing Panoptikon for first use",
    explanation: "Panoptikon is installing its local AI components. The first preparation can download several gigabytes and may take a while.",
    current: "Preparing the local AI environment…",
  },
  starting: {
    title: "Starting Panoptikon",
    explanation: "Panoptikon is starting its local Server and checking your application data.",
    current: "Starting the local Server…",
  },
  setting_up: {
    title: "Almost ready",
    explanation: "The local Server is running. Panoptikon is preparing the web interface.",
    current: "Starting the web interface…",
  },
  restarting: {
    title: "Restarting Panoptikon",
    explanation: "The local Server stopped unexpectedly. Panoptikon is recovering automatically.",
    current: "Restarting the local Server…",
  },
  stopping: {
    title: "Stopping Panoptikon",
    explanation: "Panoptikon is safely stopping its local services.",
    current: "Waiting for the local Server to stop…",
  },
};

window.updateLaunchState = (view) => {
  const copy = copyForState[view.kind] || copyForState.starting;
  byId("title").textContent = copy.title;
  byId("explanation").textContent = copy.explanation;
  byId("current").textContent = view.activity || copy.current;
  byId("diagnostics").value = view.diagnostics || "No diagnostic output yet.";
  const failed = view.kind === "failed" || view.kind === "degraded";
  byId("error").hidden = !failed;
  document.body.classList.toggle("failed", failed);
  byId("background-note").textContent = failed
    ? "You can safely close this window. Panoptikon Desktop will remain available from the tray."
    : "You can safely close this window. Panoptikon will continue in the background and notify you when it is ready.";
  if (failed) {
    byId("title").textContent = "Panoptikon needs your attention";
    byId("explanation").textContent = "Automatic startup did not complete successfully.";
    byId("current").textContent = "Startup stopped";
    byId("error-detail").textContent = view.error || "An unknown startup error occurred.";
    byId("diagnostics-panel").open = true;
  }
};

byId("copy").addEventListener("click", async () => {
  const diagnostics = byId("diagnostics");
  try {
    await navigator.clipboard.writeText(diagnostics.value);
  } catch (_) {
    diagnostics.focus();
    diagnostics.select();
    document.execCommand("copy");
  }
  byId("copy-result").textContent = "Copied";
});
