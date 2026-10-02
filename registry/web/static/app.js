// Demo Registry — vanilla JS SPA.
// Two views over the same catalog JSON:
//   /         → canonical demos (SA-facing)
//   /catalog  → ops project grid

const LINK_SOURCES = [
  { pattern: "github.com", label: "GitHub" },
  { pattern: "notion.so", label: "Notion" },
  { pattern: "docs.google.com/document", label: "Google Docs" },
  { pattern: "docs.google.com/presentation", label: "Google Slides" },
  { pattern: "drive.google.com", label: "Google Drive" },
  { pattern: "aws.amazon.com", label: "AWS" },
  { pattern: "app.gong.io", label: "Gong" },
  { pattern: "youtube.com", label: "YouTube" },
  { pattern: "youtu.be", label: "YouTube" },
];

const state = {
  projects: [],
  demos: [],
  route: window.location.pathname,
  demosSearch: "",
  selectedTags: new Set(),
  selectedSdks: new Set(),
  catalogSearch: "",
  catalogPhase: "all",
};

function linkLabel(url) {
  for (const source of LINK_SOURCES) {
    if (url && url.includes(source.pattern)) return source.label;
  }
  return "View";
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else if (key === "html") node.innerHTML = value;
    else if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else if (key === "dataset") {
      Object.assign(node.dataset, value);
    } else {
      node.setAttribute(key, value === true ? "" : value);
    }
  }
  for (const child of children) {
    if (child === null || child === undefined || child === false) continue;
    if (typeof child === "string") node.appendChild(document.createTextNode(child));
    else if (Array.isArray(child)) child.forEach((c) => c && node.appendChild(c));
    else node.appendChild(child);
  }
  return node;
}

function normalizeDemo(project) {
  const links = project.links || {};
  return {
    id: project.name,
    name: project.title || project.name,
    summary: project.summary || "",
    canonical: !!project.canonical,
    tags: Array.isArray(project.tags) ? project.tags : [],
    sdks: Array.isArray(project.temporalSdks) ? project.temporalSdks : [],
    technicalAssets: {
      repo: project.sourceRepo || undefined,
      appUrl: project.appUrl || undefined,
      runbook: links.runbook && links.runbook.url,
    },
    nonTechnicalAssets: {
      speakerNotes: links.speakerNotes && links.speakerNotes.url,
      slideDeck: links.slideDeck && links.slideDeck.url,
      goldenPath: links.goldenPath && links.goldenPath.url,
      externalRecording: links.externalRecording && links.externalRecording.url,
    },
    additional: Array.isArray(links.additional) ? links.additional : [],
    cloud: { namespace: project.temporalNamespace.includes("a2dd6") ? project.temporalNamespace : `${project.temporalNamespace}.a2dd6` },
    authRequired: !!project.authRequired,
    crashable: !!(project.crashable && project.crashable.enabled),
  };
}

async function loadData() {
  const list = document.querySelector("[data-demos-list]");
  try {
    const res = await fetch("/api/registry", { credentials: "same-origin" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    state.projects = Array.isArray(payload.projects) ? payload.projects : [];
    state.demos = state.projects.map(normalizeDemo);
  } catch (err) {
    list.replaceChildren(
      el("div", {
        class: "text-sm text-red-600 py-8 text-center",
        text: `Failed to load catalog: ${err.message}`,
      })
    );
    throw err;
  }
}

function currentView() {
  return state.route === "/catalog" ? "catalog" : "demos";
}

function setRoute(target, { replace = false } = {}) {
  if (target === state.route) return;
  state.route = target;
  const url = target + window.location.search + window.location.hash;
  if (replace) window.history.replaceState(null, "", url);
  else window.history.pushState(null, "", url);
  render();
}

function setupTabs() {
  document.querySelectorAll("[data-route]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      setRoute(link.getAttribute("data-route"));
    });
  });
  window.addEventListener("popstate", () => {
    state.route = window.location.pathname;
    render();
  });
}

// ─── Demos view ────────────────────────────────────────────────────────────

function loadDemosFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);
  state.selectedTags = new Set((params.get("tags") || "").split(",").filter(Boolean));
  state.selectedSdks = new Set((params.get("sdks") || "").split(",").filter(Boolean));
  state.demosSearch = params.get("q") || "";
}

function syncDemosFiltersToUrl() {
  const params = new URLSearchParams(window.location.search);
  if (state.selectedTags.size) params.set("tags", [...state.selectedTags].join(","));
  else params.delete("tags");
  if (state.selectedSdks.size) params.set("sdks", [...state.selectedSdks].join(","));
  else params.delete("sdks");
  if (state.demosSearch.trim()) params.set("q", state.demosSearch.trim());
  else params.delete("q");
  const q = params.toString();
  const url = window.location.pathname + (q ? `?${q}` : "") + window.location.hash;
  window.history.replaceState(null, "", url);
}

function setupDemosFilters() {
  const search = document.querySelector("[data-demos-search]");
  search.value = state.demosSearch;
  search.addEventListener("input", (event) => {
    state.demosSearch = event.target.value;
    syncDemosFiltersToUrl();
    renderDemosList();
  });
  document.querySelector("[data-clear-demos-filters]").addEventListener("click", () => {
    state.demosSearch = "";
    state.selectedTags.clear();
    state.selectedSdks.clear();
    search.value = "";
    syncDemosFiltersToUrl();
    renderDemosFilters();
    renderDemosList();
  });
}

function renderDemosFilters() {
  const canonical = state.demos.filter((d) => d.canonical);
  const usedTags = new Set();
  const usedSdks = new Set();
  for (const demo of canonical) {
    demo.tags.forEach((t) => usedTags.add(t));
    demo.sdks.forEach((s) => usedSdks.add(s));
  }
  const tagsRow = document.querySelector("[data-tags-row]");
  const sdksRow = document.querySelector("[data-sdks-row]");
  const tagsLabel = tagsRow.firstElementChild;
  const sdksLabel = sdksRow.firstElementChild;
  tagsRow.replaceChildren(tagsLabel, ...[...usedTags].sort().map((tag) => chip(tag, "tag")));
  sdksRow.replaceChildren(sdksLabel, ...[...usedSdks].sort().map((sdk) => chip(sdk, "sdk")));
  const hasFilters =
    state.selectedTags.size || state.selectedSdks.size || state.demosSearch.trim();
  document.querySelector("[data-clear-demos-filters]").hidden = !hasFilters;
}

function chip(value, kind) {
  const isSelected =
    kind === "tag" ? state.selectedTags.has(value) : state.selectedSdks.has(value);
  const base =
    "inline-flex items-center rounded-md px-2.5 py-0.5 text-xs font-medium cursor-pointer transition-colors border";
  const selected = "bg-foreground text-background border-foreground";
  const unselected = "bg-muted text-foreground border-border hover:bg-muted/70";
  return el(
    "button",
    {
      type: "button",
      class: `${base} ${isSelected ? selected : unselected}`,
      onclick: () => {
        const target = kind === "tag" ? state.selectedTags : state.selectedSdks;
        if (target.has(value)) target.delete(value);
        else target.add(value);
        syncDemosFiltersToUrl();
        renderDemosFilters();
        renderDemosList();
      },
    },
    value
  );
}

function filteredDemos() {
  const q = state.demosSearch.trim().toLowerCase();
  return state.demos
    .filter((d) => d.canonical)
    .filter((demo) => {
      const matchesTags =
        state.selectedTags.size === 0 ||
        [...state.selectedTags].every((t) => demo.tags.includes(t));
      const matchesSdks =
        state.selectedSdks.size === 0 ||
        [...state.selectedSdks].some((s) => demo.sdks.includes(s));
      const matchesSearch =
        !q ||
        demo.name.toLowerCase().includes(q) ||
        demo.summary.toLowerCase().includes(q) ||
        demo.tags.some((t) => t.toLowerCase().includes(q)) ||
        demo.sdks.some((s) => s.toLowerCase().includes(q));
      return matchesTags && matchesSdks && matchesSearch;
    });
}

function renderDemosList() {
  const list = document.querySelector("[data-demos-list]");
  const totalCanonical = state.demos.filter((d) => d.canonical).length;
  const demos = filteredDemos();
  const count = document.querySelector("[data-demos-count]");
  const hasFilters =
    state.selectedTags.size || state.selectedSdks.size || state.demosSearch.trim();
  if (hasFilters) {
    count.hidden = false;
    count.textContent = `Showing ${demos.length} of ${totalCanonical} demo${
      totalCanonical === 1 ? "" : "s"
    }`;
  } else {
    count.hidden = true;
  }
  if (demos.length === 0) {
    list.replaceChildren(
      el(
        "div",
        { class: "text-center py-8 space-y-2" },
        el("p", {
          class: "text-sm text-muted-foreground",
          text: totalCanonical === 0
            ? "No canonical demos yet. Mark a project canonical in its YAML to list it here."
            : "No demos match the selected filters.",
        })
      )
    );
    return;
  }
  clearAllWorkspaceTimers();
  list.replaceChildren(...demos.map(renderDemoRow));
  if (window.location.hash) {
    const id = window.location.hash.slice(1);
    requestAnimationFrame(() => {
      const node = document.getElementById(id);
      if (node) {
        const details = node.querySelector("details");
        if (details) details.open = true;
        node.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  }
}

function authRequiredChip() {
  return el(
    "span",
    {
      class:
        "group/auth relative inline-flex items-center gap-1 rounded-md border border-amber-300 bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-700 cursor-help",
      tabindex: "0",
      "aria-label": "Auth required: sign in with a @temporal.io email to access this demo",
      onclick: (event) => event.preventDefault(),
    },
    el("span", { class: "select-none", text: "🔒" }),
    el("span", { text: "Auth required" }),
    el("span", {
      class:
        "pointer-events-none absolute left-0 top-full z-10 mt-1.5 w-60 rounded-md bg-foreground px-2.5 py-1.5 text-xs font-normal leading-snug text-background opacity-0 shadow-md transition-opacity duration-150 group-hover/auth:opacity-100 group-focus/auth:opacity-100",
      text: "Requires signing in with a @temporal.io email to access this demo.",
    })
  );
}

function summaryTags(tags) {
  if (!tags.length) return null;
  return el(
    "div",
    { class: "flex flex-wrap gap-1.5 mt-2" },
    ...tags.map((tag) =>
      el("span", {
        class:
          "inline-flex items-center rounded-md bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground",
        text: tag,
      })
    )
  );
}

function renderDemoRow(demo) {
  const titleRow = el(
    "div",
    { class: "flex items-center gap-3 mb-1" },
    el("h2", { class: "text-lg font-semibold", text: demo.name }),
    copyLinkButton(demo.id),
    demo.authRequired ? authRequiredChip() : null
  );
  const summary = el(
    "summary",
    { class: "px-6 py-4 cursor-pointer list-none flex items-start gap-4" },
    el(
      "div",
      { class: "flex-1 min-w-0" },
      titleRow,
      el("p", { class: "text-sm text-muted-foreground", text: demo.summary }),
      summaryTags(demo.tags)
    ),
    el("span", {
      class:
        "text-muted-foreground text-xl select-none transition-transform duration-200 group-open:rotate-90",
      text: "›",
    })
  );

  const quickLaunch = el(
    "div",
    { class: "grid grid-cols-1 md:grid-cols-3 gap-3 mb-6" },
    quickLaunchTile("App UI", demo.technicalAssets.appUrl, "Open Demo"),
    quickLaunchTile(
      "Temporal Cloud",
      demo.cloud.namespace
        ? `https://cloud.temporal.io/namespaces/${demo.cloud.namespace}/workflows`
        : null,
      "Open Namespace"
    ),
    quickLaunchTile("Speaker Notes", demo.nonTechnicalAssets.speakerNotes, "View Notes")
  );

  const tagsAndSdks = el(
    "div",
    { class: "mb-6" },
    el("h3", {
      class: "text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3",
      text: "Temporal SDKs",
    }),
    el(
      "div",
      { class: "flex flex-wrap gap-1.5" },
      ...demo.sdks.map((sdk) =>
        el("span", {
          class:
            "inline-flex items-center rounded-md border border-border px-2 py-0.5 text-xs font-medium",
          text: sdk,
        })
      )
    )
  );

  const technicalAdditional = demo.additional.filter((a) => a.audience === "technical");
  const nonTechnicalAdditional = demo.additional.filter((a) => a.audience === "nonTechnical");

  const assets = el(
    "div",
    { class: "grid grid-cols-1 md:grid-cols-2 gap-8" },
    el(
      "div",
      {},
      el("h3", {
        class: "text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3",
        text: "Technical Assets",
      }),
      el(
        "div",
        { class: "divide-y divide-border" },
        linkItem("Repository", demo.technicalAssets.repo),
        linkItem("Local Runbook", demo.technicalAssets.runbook),
        ...technicalAdditional.map((a) => linkItem(a.label, a.url))
      )
    ),
    el(
      "div",
      {},
      el("h3", {
        class: "text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3",
        text: "Non-Technical Assets",
      }),
      el(
        "div",
        { class: "divide-y divide-border" },
        linkItem("Slide Deck", demo.nonTechnicalAssets.slideDeck),
        linkItem("Golden Path Recording", demo.nonTechnicalAssets.goldenPath),
        linkItem("External Recording", demo.nonTechnicalAssets.externalRecording),
        ...nonTechnicalAdditional.map((a) => linkItem(a.label, a.url))
      )
    )
  );

  const body = el(
    "div",
    { class: "px-6 pb-6 details-body" },
    el("hr", { class: "border-border mb-5" }),
    quickLaunch,
    demo.crashable ? crashableWorkspaceCard(demo) : null,
    tagsAndSdks,
    assets
  );
  const details = el("details", { class: "group" }, summary, body);
  summary.addEventListener("click", () => {
    if (details.open) return; // about to close — no open animation
    // Arm the slide-down before the browser paints the opened state, so the
    // content never flashes at full opacity first. Re-trigger on every open.
    body.classList.remove("details-animate");
    void body.offsetWidth; // force reflow so the animation restarts
    body.classList.add("details-animate");
  });

  return el(
    "article",
    {
      id: demo.id,
      class: "rounded-lg border border-border bg-card shadow-sm scroll-mt-4",
    },
    details
  );
}

function quickLaunchTile(label, url, activeText) {
  const base =
    "flex flex-col items-center justify-center rounded-lg border p-4 text-center transition-colors";
  if (url) {
    return el(
      "a",
      {
        href: url,
        target: "_blank",
        rel: "noopener noreferrer",
        class: `${base} border-primary/30 bg-primary-soft hover:bg-primary/10`,
      },
      el("span", {
        class:
          "text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-1",
        text: label,
      }),
      el("span", { class: "text-sm font-medium", text: activeText })
    );
  }
  return el(
    "div",
    { class: `${base} border-border bg-muted/30 opacity-50` },
    el("span", {
      class:
        "text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-1",
      text: label,
    }),
    el("span", { class: "text-sm font-medium", text: "Not Available" })
  );
}

function linkItem(label, url) {
  if (!url) {
    return el(
      "div",
      { class: "flex items-center justify-between gap-4 py-1.5" },
      el("span", { class: "text-sm text-muted-foreground truncate", text: label }),
      el("span", { class: "text-xs text-muted-foreground italic shrink-0", text: "—" })
    );
  }
  return el(
    "div",
    { class: "flex items-center justify-between gap-4 py-1.5" },
    el("span", { class: "text-sm truncate", title: label, text: label }),
    el(
      "a",
      {
        href: url,
        target: "_blank",
        rel: "noopener noreferrer",
        class:
          "text-xs text-primary underline underline-offset-2 hover:text-primary/80 shrink-0",
      },
      linkLabel(url)
    )
  );
}

function copyLinkButton(id) {
  const icon = el("span", { class: "text-sm leading-none", text: "🔗" });
  const label = el("span", { class: "text-xs font-medium", text: "Copy Registry Link" });
  const button = el(
    "button",
    {
      type: "button",
      class:
        "inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-2 py-1 text-muted-foreground hover:bg-muted hover:text-foreground hover:border-foreground/30 cursor-pointer transition-colors",
      title: "Copy deep link to this demo",
      "aria-label": "Copy deep link to this demo",
    },
    icon,
    label
  );
  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    const url = `${window.location.origin}/#${id}`;
    window.history.replaceState(null, "", `/#${id}`);
    if (navigator.clipboard) navigator.clipboard.writeText(url);
    icon.textContent = "✓";
    label.textContent = "Copied!";
    button.classList.add("text-accent", "border-accent/40");
    setTimeout(() => {
      icon.textContent = "🔗";
      label.textContent = "Copy link";
      button.classList.remove("text-accent", "border-accent/40");
    }, 1500);
  });
  return button;
}

// ─── Crashable workspace ───────────────────────────────────────────────────
// A per-(user, demo) ephemeral clone of the demo's Kubernetes namespace, driven
// by a Temporal entity workflow. The card renders from a client-side cache of
// the workflow's status query and re-arms its own poll/countdown timers on each
// render. Because renderDemosList() rebuilds every row (destroying nodes) on any
// filter change, timers are keyed by demo id and cleared centrally there; each
// re-rendered card looks itself up in the DOM via [data-workspace=<id>]. The
// card renders in both the demos rows and the catalog cards; hidden views keep
// their nodes, so lookups are scoped to the visible view.

const workspaceState = new Map(); // demoId -> last known status object
const workspaceTimers = new Map(); // demoId -> [intervalId, ...]
const workspaceFetching = new Set(); // demoIds with an in-flight initial fetch

function clearWorkspaceTimers(id) {
  const timers = workspaceTimers.get(id);
  if (timers) {
    timers.forEach((timerId) => clearInterval(timerId));
    workspaceTimers.delete(id);
  }
}

function clearAllWorkspaceTimers() {
  for (const id of [...workspaceTimers.keys()]) clearWorkspaceTimers(id);
}

function setWorkspaceTimer(id, timerId) {
  const timers = workspaceTimers.get(id) || [];
  timers.push(timerId);
  workspaceTimers.set(id, timers);
}

function workspaceContainer(id) {
  return document.querySelector(
    `[data-view="${currentView()}"] [data-workspace="${id}"]`
  );
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  let data = {};
  try {
    data = await res.json();
  } catch (err) {
    data = {};
  }
  if (!res.ok) {
    const error = new Error(data.message || data.error || `HTTP ${res.status}`);
    error.status = res.status;
    throw error;
  }
  return data;
}

async function fetchWorkspaceStatus(demoId) {
  const res = await fetch(
    `/api/crashable-workspace?demo=${encodeURIComponent(demoId)}`,
    { credentials: "same-origin" }
  );
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function formatCountdown(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const mm = String(m).padStart(2, "0");
  const ss = String(s).padStart(2, "0");
  return h > 0 ? `${h}:${mm}:${ss}` : `${m}:${ss}`;
}

function workspaceSpinner() {
  return el("span", {
    class:
      "inline-block h-4 w-4 shrink-0 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent",
  });
}

function workspaceButton(label, cls, onclick) {
  return el(
    "button",
    {
      type: "button",
      class: `inline-flex items-center justify-center gap-1.5 rounded-md px-3 py-2 text-sm font-medium transition-colors cursor-pointer ${cls}`,
      onclick: (event) => {
        event.preventDefault();
        event.stopPropagation();
        onclick();
      },
    },
    label
  );
}

async function workspaceAction(demo, url) {
  try {
    const status = await postJSON(url, { demo: demo.id });
    workspaceState.set(demo.id, status);
  } catch (err) {
    const prev = workspaceState.get(demo.id) || { phase: "None", demo: demo.id };
    workspaceState.set(demo.id, { ...prev, lastActionError: err.message });
  }
  const node = workspaceContainer(demo.id);
  if (node) renderWorkspaceInto(demo, node);
}

function pollWorkspaceOnce(demo) {
  fetchWorkspaceStatus(demo.id)
    .then((status) => workspaceState.set(demo.id, status))
    .catch(() => {})
    .finally(() => {
      const node = workspaceContainer(demo.id);
      if (node) renderWorkspaceInto(demo, node);
    });
}

// Poll while the workflow is doing async work (provisioning, outage actions,
// and deletion).
function armWorkspacePolling(demo) {
  return setInterval(() => {
    const node = workspaceContainer(demo.id);
    if (!node) {
      clearWorkspaceTimers(demo.id);
      return;
    }
    fetchWorkspaceStatus(demo.id)
      .then((status) => {
        const prev = workspaceState.get(demo.id) || {};
        workspaceState.set(demo.id, status);
        const current = workspaceContainer(demo.id);
        if (!current) {
          clearWorkspaceTimers(demo.id);
          return;
        }
        if (status.phase !== prev.phase) {
          renderWorkspaceInto(demo, current); // re-arms timers for the new phase
        } else {
          const stepNode = current.querySelector("[data-workspace-step]");
          if (stepNode && status.step) stepNode.textContent = status.step;
        }
      })
      .catch(() => {});
  }, 2000);
}

// Tick the visible countdown once per second while the workspace is settled;
// re-check status when it reaches zero (the workflow auto-deletes on expiry).
function armWorkspaceCountdown(demo, expiresAt) {
  return setInterval(() => {
    const node = workspaceContainer(demo.id);
    if (!node) {
      clearWorkspaceTimers(demo.id);
      return;
    }
    if (!expiresAt) return;
    const remaining = expiresAt - Date.now();
    const cd = node.querySelector("[data-workspace-countdown]");
    if (cd) cd.textContent = formatCountdown(remaining);
    if (remaining <= 0) {
      clearWorkspaceTimers(demo.id);
      pollWorkspaceOnce(demo);
    }
  }, 1000);
}

function crashableWorkspaceCard(demo, { className = "mb-6" } = {}) {
  const container = el("div", {
    class: className,
    dataset: { workspace: demo.id },
  });
  renderWorkspaceInto(demo, container);
  if (!workspaceState.has(demo.id) && !workspaceFetching.has(demo.id)) {
    workspaceFetching.add(demo.id);
    fetchWorkspaceStatus(demo.id)
      .then((status) => workspaceState.set(demo.id, status))
      .catch(() => workspaceState.set(demo.id, { phase: "None", demo: demo.id }))
      .finally(() => {
        workspaceFetching.delete(demo.id);
        const node = workspaceContainer(demo.id);
        if (node) renderWorkspaceInto(demo, node);
      });
  }
  return container;
}

function renderWorkspaceInto(demo, container) {
  clearWorkspaceTimers(demo.id);
  const status = workspaceState.get(demo.id) || null;
  const phase = status
    ? status.phase
    : workspaceFetching.has(demo.id)
    ? "Loading"
    : "None";

  const heading = el(
    "h3",
    {
      class:
        "text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3",
      text: "Crashable Environment",
    }
  );
  const children = [el("hr", { class: "border-border mb-5" }), heading];
  const note = (text) =>
    el("p", { class: "text-sm text-muted-foreground", text });

  if (phase === "Loading") {
    children.push(note("Checking for an existing environment…"));
  } else if (phase === "None") {
    children.push(
      note(
        "Spin up a private, time-limited copy of this demo’s Kubernetes workers that you can crash or scale down to watch Temporal recover. Auto-deletes after 1 hour."
      ),
      el(
        "div",
        { class: "mt-3" },
        workspaceButton(
          "Create Crashable Environment",
          "bg-foreground text-background hover:bg-foreground/90",
          () => workspaceAction(demo, "/api/crashable-workspace")
        )
      )
    );
  } else if (phase === "Provisioning") {
    children.push(
      el(
        "div",
        { class: "flex items-center gap-3" },
        workspaceSpinner(),
        el(
          "div",
          {},
          el("p", { class: "text-sm font-medium", text: "Creating your environment…" }),
          el("p", {
            class: "text-xs text-muted-foreground",
            dataset: { workspaceStep: "" },
            text: (status && status.step) || "Starting…",
          })
        )
      )
    );
    setWorkspaceTimer(demo.id, armWorkspacePolling(demo));
  } else if (
    phase === "Crashing" ||
    phase === "ScalingDown" ||
    phase === "ScalingUp"
  ) {
    children.push(
      el(
        "div",
        { class: "flex items-center gap-3" },
        workspaceSpinner(),
        el("p", {
          class: "text-sm font-medium",
          dataset: { workspaceStep: "" },
          text:
            (status && status.step) ||
            (phase === "ScalingDown"
              ? "Scaling workers to zero…"
              : phase === "ScalingUp"
              ? "Scaling workers to one…"
              : "Crashing workers…"),
        })
      )
    );
    setWorkspaceTimer(demo.id, armWorkspacePolling(demo));
  } else if (phase === "Deleting") {
    children.push(
      el(
        "div",
        { class: "flex items-center gap-3" },
        workspaceSpinner(),
        el("p", { class: "text-sm font-medium", text: "Deleting environment…" })
      )
    );
    setWorkspaceTimer(demo.id, armWorkspacePolling(demo));
  } else if (phase === "Failed") {
    children.push(
      el("p", {
        class: "text-sm text-red-600",
        text: (status && status.last_error) || "Environment provisioning failed.",
      }),
      el(
        "div",
        { class: "mt-3 flex flex-wrap gap-2" },
        workspaceButton(
          "Try Again",
          "bg-foreground text-background hover:bg-foreground/90",
          () => workspaceAction(demo, "/api/crashable-workspace")
        ),
        workspaceButton(
          "Dismiss",
          "border border-border bg-background text-muted-foreground hover:text-foreground",
          () => workspaceAction(demo, "/api/crashable-workspace/delete")
        )
      )
    );
  } else if (phase === "Active") {
    const appUrl = (status && status.app_url) || null;
    const expiresAt =
      status && status.expires_at ? Date.parse(status.expires_at) : null;
    children.push(
      el(
        "div",
        { class: "flex flex-wrap items-center gap-2" },
        appUrl
          ? el(
              "a",
              {
                href: appUrl,
                target: "_blank",
                rel: "noopener noreferrer",
                class:
                  "inline-flex items-center justify-center rounded-md border border-primary/30 bg-primary-soft px-3 py-2 text-sm font-medium hover:bg-primary/10",
              },
              "Open Environment"
            )
          : null,
        workspaceButton("Crash Workers", "bg-red-600 text-white hover:bg-red-700", () =>
          workspaceAction(demo, "/api/crashable-workspace/crash")
        ),
        workspaceButton(
          "Scale Down Workers",
          "border border-amber-500/50 bg-amber-500/10 text-amber-700 hover:bg-amber-500/20",
          () => workspaceAction(demo, "/api/crashable-workspace/scale-down")
        ),
        workspaceButton(
          "Extend +1h",
          "border border-border bg-background hover:bg-muted",
          () => workspaceAction(demo, "/api/crashable-workspace/extend")
        ),
        workspaceButton(
          "Delete",
          "border border-border bg-background text-muted-foreground hover:text-foreground",
          () => workspaceAction(demo, "/api/crashable-workspace/delete")
        )
      ),
      el(
        "div",
        { class: "mt-2 text-xs text-muted-foreground" },
        "Auto-deletes in ",
        el("span", {
          class: "font-mono font-medium text-foreground",
          dataset: { workspaceCountdown: "" },
          text: expiresAt ? formatCountdown(expiresAt - Date.now()) : "—",
        })
      ),
      status && status.namespace
        ? el("p", {
            class: "mt-1 text-[11px] text-muted-foreground font-mono",
            text: status.namespace,
          })
        : null,
      status && status.last_error
        ? el("p", {
            class: "mt-2 text-xs text-red-600",
            text: status.last_error,
          })
        : null
    );
    setWorkspaceTimer(demo.id, armWorkspaceCountdown(demo, expiresAt));
  } else if (phase === "ScaledDown") {
    const expiresAt =
      status && status.expires_at ? Date.parse(status.expires_at) : null;
    children.push(
      note(
        "Worker pods are scaled to zero. Application pods remain online while Temporal work waits durably for the workers to return."
      ),
      el(
        "div",
        { class: "mt-3 flex flex-wrap items-center gap-2" },
        workspaceButton(
          "Scale Up Workers",
          "bg-foreground text-background hover:bg-foreground/90",
          () => workspaceAction(demo, "/api/crashable-workspace/scale-up")
        ),
        workspaceButton(
          "Extend +1h",
          "border border-border bg-background hover:bg-muted",
          () => workspaceAction(demo, "/api/crashable-workspace/extend")
        ),
        workspaceButton(
          "Delete",
          "border border-border bg-background text-muted-foreground hover:text-foreground",
          () => workspaceAction(demo, "/api/crashable-workspace/delete")
        )
      ),
      el(
        "div",
        { class: "mt-2 text-xs text-muted-foreground" },
        "Auto-deletes in ",
        el("span", {
          class: "font-mono font-medium text-foreground",
          dataset: { workspaceCountdown: "" },
          text: expiresAt ? formatCountdown(expiresAt - Date.now()) : "—",
        })
      ),
      status && status.namespace
        ? el("p", {
            class: "mt-1 text-[11px] text-muted-foreground font-mono",
            text: status.namespace,
          })
        : null,
      status && status.last_error
        ? el("p", {
            class: "mt-2 text-xs text-red-600",
            text: status.last_error,
          })
        : null
    );
    setWorkspaceTimer(demo.id, armWorkspaceCountdown(demo, expiresAt));
  }

  if (status && status.lastActionError) {
    children.push(
      el("p", { class: "mt-2 text-xs text-red-600", text: status.lastActionError })
    );
  }

  container.replaceChildren(...children.filter(Boolean));
}

// ─── Catalog view ──────────────────────────────────────────────────────────

function loadCatalogFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);
  state.catalogSearch = params.get("q") || "";
  state.catalogPhase = params.get("phase") || "all";
}

function syncCatalogFiltersToUrl() {
  const params = new URLSearchParams(window.location.search);
  if (state.catalogSearch.trim()) params.set("q", state.catalogSearch.trim());
  else params.delete("q");
  if (state.catalogPhase && state.catalogPhase !== "all") params.set("phase", state.catalogPhase);
  else params.delete("phase");
  const q = params.toString();
  const url = window.location.pathname + (q ? `?${q}` : "") + window.location.hash;
  window.history.replaceState(null, "", url);
}

function setupCatalogFilters() {
  const search = document.querySelector("[data-catalog-search]");
  search.value = state.catalogSearch;
  search.addEventListener("input", (event) => {
    state.catalogSearch = event.target.value;
    syncCatalogFiltersToUrl();
    renderCatalogList();
  });
  document.querySelector("[data-catalog-phase]").addEventListener("change", (event) => {
    state.catalogPhase = event.target.value;
    syncCatalogFiltersToUrl();
    renderCatalogList();
  });
}

function projectPhase(project) {
  const phase = project.status && project.status.phase;
  return typeof phase === "string" && phase.trim() ? phase : "Catalogued";
}

function renderCatalogPhaseOptions() {
  const select = document.querySelector("[data-catalog-phase]");
  const phases = [...new Set(state.projects.map(projectPhase))].sort();
  select.replaceChildren(
    new Option("All phases", "all"),
    ...phases.map((phase) => new Option(phase, phase))
  );
  select.value = state.catalogPhase;
}

function matchesCatalog(project) {
  if (state.catalogPhase !== "all" && projectPhase(project) !== state.catalogPhase) {
    return false;
  }
  const q = state.catalogSearch.trim().toLowerCase();
  if (!q) return true;
  const haystack = [
    project.name,
    project.title,
    project.summary,
    project.sourceRepo,
    ...(Array.isArray(project.tags) ? project.tags : []),
    ...(Array.isArray(project.temporalSdks) ? project.temporalSdks : []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(q);
}

function renderCatalogList() {
  const list = document.querySelector("[data-catalog-list]");
  const visible = state.projects.filter(matchesCatalog);
  const count = document.querySelector("[data-catalog-count]");
  if (state.catalogSearch.trim() || state.catalogPhase !== "all") {
    count.hidden = false;
    count.textContent = `Showing ${visible.length} of ${state.projects.length} project${
      state.projects.length === 1 ? "" : "s"
    }`;
  } else {
    count.hidden = true;
  }
  if (visible.length === 0) {
    list.replaceChildren(
      el("div", {
        class: "text-sm text-muted-foreground py-8 text-center col-span-full",
        text: "No matching projects.",
      })
    );
    return;
  }
  clearAllWorkspaceTimers();
  list.replaceChildren(...visible.map(renderProjectCard));
}

function renderProjectCard(project) {
  const phase = projectPhase(project);
  const title = project.title || project.name;
  const summary =
    (project.status && project.status.message && project.status.message.trim()) ||
    project.summary ||
    "No summary provided.";
  return el(
    "article",
    { class: "rounded-lg border border-border bg-card shadow-sm p-5 flex flex-col gap-3" },
    el(
      "div",
      { class: "flex items-start justify-between gap-3" },
      el("h2", { class: "text-base font-semibold", text: title }),
      el("span", {
        class: "inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground",
        text: phase,
      })
    ),
    el("p", { class: "text-sm text-muted-foreground", text: summary }),
    Array.isArray(project.tags) && project.tags.length
      ? el(
          "div",
          { class: "flex flex-wrap gap-1.5" },
          ...project.tags.map((tag) =>
            el("span", {
              class: "inline-flex items-center rounded-md bg-muted px-2 py-0.5 text-xs",
              text: tag,
            })
          )
        )
      : null,
    project.crashable && project.crashable.enabled
      ? crashableWorkspaceCard({ id: project.name }, { className: "" })
      : null,
    el(
      "div",
      { class: "flex items-center gap-4 text-xs text-muted-foreground mt-auto pt-2" },
      project.host
        ? el(
            "a",
            {
              href: project.appUrl || `https://${project.host}`,
              target: "_blank",
              rel: "noopener noreferrer",
              class: "text-primary underline underline-offset-2 hover:text-primary/80",
            },
            project.host
          )
        : el("span", { text: "—" }),
      project.sourceRepo
        ? el(
            "a",
            {
              href: project.sourceRepo,
              target: "_blank",
              rel: "noopener noreferrer",
              class: "underline underline-offset-2 hover:text-foreground",
            },
            "Source"
          )
        : null,
      project.authRequired
        ? el("span", { class: "ml-auto text-[11px]", text: "Auth required" })
        : null
    )
  );
}

// ─── Render orchestration ─────────────────────────────────────────────────

function render() {
  document.querySelectorAll("[data-view]").forEach((section) => {
    section.hidden = section.getAttribute("data-view") !== currentView();
  });
  document.querySelectorAll("[data-route]").forEach((link) => {
    if (link.getAttribute("data-route") === state.route) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });
  if (currentView() === "demos") {
    renderDemosFilters();
    renderDemosList();
  } else {
    renderCatalogPhaseOptions();
    renderCatalogList();
  }
}

// ─── Boot ─────────────────────────────────────────────────────────────────

async function init() {
  loadDemosFiltersFromUrl();
  loadCatalogFiltersFromUrl();
  setupTabs();
  setupDemosFilters();
  setupCatalogFilters();
  await loadData();
  render();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
