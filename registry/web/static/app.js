// Demo Registry — vanilla JS SPA.
// Two views over the same catalog JSON, plus onboarding:
//   /         → demos (canonical feature cards + a grid of everything else;
//               each demo opens in a detail drawer, deep-linked as /#<id>)
//   /courses  → Instruqt workshops (the server routes this path to the SPA)
//   /onboard  → add a demo to the catalog (the user's onboarding workflow)

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

const WORKSHOPS_ROUTE = "/courses";
const ONBOARD_ROUTE = "/onboard";
// Instruqt access is granted through Serval, our internal onboarding tool.
const INSTRUQT_ACCESS_URL =
  "https://app.serval.com/catalog?request=access-catalog&appId=019f4bef-e156-742c-bbac-f363c6cc2937";

const state = {
  demos: [],
  courses: [],
  route: window.location.pathname,
  demosSearch: "",
  selectedTags: new Set(),
  selectedSdks: new Set(),
  selectedDemoAiSdks: new Set(),
  workshopsSearch: "",
  selectedWorkshopSdks: new Set(),
  selectedWorkshopAiSdks: new Set(),
  workshopHours: null,
  openDrawerId: null,
  // Ziggy's view of the demos page: the demos it filtered to (null: no
  // filter) and the ones it put a halo on. Not in the URL; the chat owns it.
  ziggyPicks: null,
  ziggyHighlights: new Set(),
};

const ICONS = {
  external: '<path d="M7 4h9v9"/><path d="M16 4 5 15"/>',
  lock: '<rect x="4" y="9" width="12" height="8" rx="2"/><path d="M7 9V6a3 3 0 0 1 6 0v3"/>',
  bolt: '<path d="M11 2 4 11h5l-1 7 7-9h-5z"/>',
  stack: '<path d="M10 3 3 7l7 4 7-4-7-4z"/><path d="m3 10 7 4 7-4"/><path d="m3 13 7 4 7-4"/>',
  star: '<path d="m10 2.5 2.3 4.8 5.2.7-3.8 3.6.9 5.2L10 14.3l-4.6 2.5.9-5.2L2.5 8l5.2-.7z"/>',
  link: '<path d="M8.5 11.5a3.5 3.5 0 0 0 5 0l2.5-2.5a3.5 3.5 0 0 0-5-5L10 5"/><path d="M11.5 8.5a3.5 3.5 0 0 0-5 0L4 11a3.5 3.5 0 0 0 5 5l1-1"/>',
  close: '<path d="m5 5 10 10M15 5 5 15"/>',
  arrow: '<path d="M4 10h12M11 5l5 5-5 5"/>',
  sparkle: '<path d="M10 1c.8 5.2 3.8 8.2 9 9-5.2.8-8.2 3.8-9 9-.8-5.2-3.8-8.2-9-9 5.2-.8 8.2-3.8 9-9Z"/>',
};

const SPARKLE_PATH =
  "M20 0c1.6 11 9 18.4 20 20-11 1.6-18.4 9-20 20C18.4 29 11 21.6 0 20 11 18.4 18.4 11 20 0Z";

function icon(name) {
  const span = document.createElement("span");
  span.style.display = "contents";
  span.innerHTML = `<svg viewBox="0 0 20 20" aria-hidden="true">${ICONS[name]}</svg>`;
  return span.firstChild;
}

function linkLabel(url) {
  for (const source of LINK_SOURCES) {
    if (url && url.includes(source.pattern)) return source.label;
  }
  return "Open";
}

function plural(n, word) {
  return `${n} ${word}${n === 1 ? "" : "s"}`;
}

// Per-SDK accent colors, matching the language colors on temporal.io's Code
// Exchange cards (`ink` is the readable text color on top of `color`).
const SDK_META = {
  python: { abbr: "PY", color: "#fecb2f", ink: "#141414" },
  typescript: { abbr: "TS", color: "#00f986", ink: "#141414" },
  go: { abbr: "GO", color: "#444ce7", ink: "#f8fafc" },
  java: { abbr: "JV", color: "#ff6bff", ink: "#141414" },
  ".net": { abbr: ".N", color: "#9747ff", ink: "#f8fafc" },
  rust: { abbr: "RS", color: "#d34516", ink: "#f8fafc" },
  php: { abbr: "PH", color: "#7f86f1", ink: "#141414" },
  ruby: { abbr: "RB", color: "#ff5555", ink: "#f8fafc" },
};
const SDK_ALIASES = { dotnet: ".net", "c#": ".net", ts: "typescript", golang: "go" };

function sdkMeta(sdk) {
  const key = String(sdk).trim().toLowerCase();
  return (
    SDK_META[SDK_ALIASES[key] || key] || {
      abbr: key.replace(/[^a-z0-9.]/g, "").slice(0, 2).toUpperCase(),
      color: "#667ca1",
      ink: "#f8fafc",
    }
  );
}

function sdkStyle(sdk) {
  const meta = sdkMeta(sdk);
  return `--sdk:${meta.color};--sdk-ink:${meta.ink}`;
}

function sdkMark(sdk) {
  return el("span", {
    class: "sdk-mark",
    style: sdkStyle(sdk),
    "aria-hidden": "true",
    text: sdkMeta(sdk).abbr,
  });
}

// AI SDK accents. Marks are outlined rather than filled so they read apart from
// the Temporal SDK marks at the same size. Unknown names fall back to initials.
const AI_SDK_META = {
  "anthropic sdk": { abbr: "AN", color: "#d97757" },
  "openai sdk": { abbr: "OA", color: "#10a37f" },
  "openai agents sdk": { abbr: "OA", color: "#10a37f" },
  "google adk": { abbr: "AD", color: "#4285f4" },
  "google genai sdk": { abbr: "GG", color: "#4285f4" },
  langgraph: { abbr: "LG", color: "#8b5cf6" },
  "spring ai": { abbr: "SA", color: "#6db33f" },
  "pydantic ai": { abbr: "PA", color: "#e92063" },
};

function aiSdkMeta(aiSdk) {
  const key = String(aiSdk).trim().toLowerCase();
  return (
    AI_SDK_META[key] || {
      abbr: key.replace(/[^a-z0-9]/g, "").slice(0, 2).toUpperCase(),
      color: "#667ca1",
    }
  );
}

function aiSdkMark(aiSdk) {
  return el("span", {
    class: "sdk-mark ai-sdk-mark",
    style: `--sdk:${aiSdkMeta(aiSdk).color}`,
    "aria-hidden": "true",
    text: aiSdkMeta(aiSdk).abbr,
  });
}

// Temporal SDKs first, then the AI SDKs after a divider. Workshops pass only
// their Temporal SDKs.
function sdkList(sdks, aiSdks = []) {
  if (!sdks.length && !aiSdks.length) return null;
  const temporal = sdks.map((sdk) => el("span", { class: "sdk" }, sdkMark(sdk), sdk));
  const ai = aiSdks.map((aiSdk) =>
    el("span", { class: "sdk ai-sdk", title: "AI SDK" }, aiSdkMark(aiSdk), aiSdk)
  );
  return el(
    "div",
    { class: "sdk-list" },
    ...temporal,
    temporal.length && ai.length ? el("span", { class: "sdk-divider", "aria-hidden": "true" }) : null,
    ...ai
  );
}

// The colored base stripe on a demo card: one equal, hard-edged segment per SDK.
function sdkBar(sdks) {
  const colors = sdks.map((sdk) => sdkMeta(sdk).color);
  if (!colors.length) return "var(--gray-500)";
  if (colors.length === 1) return colors[0];
  const step = 100 / colors.length;
  const stops = colors.map(
    (color, i) => `${color} ${(i * step).toFixed(2)}% ${((i + 1) * step).toFixed(2)}%`
  );
  return `linear-gradient(90deg, ${stops.join(", ")})`;
}

function sparkle(style) {
  const wrap = document.createElement("span");
  wrap.innerHTML = `<svg class="sparkle" viewBox="0 0 40 40" aria-hidden="true"><path d="${SPARKLE_PATH}"/></svg>`;
  const svg = wrap.firstChild;
  if (style) svg.setAttribute("style", style);
  return svg;
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

function externalLink(href, attrs, ...children) {
  return el("a", { href, target: "_blank", rel: "noopener noreferrer", ...attrs }, ...children);
}

function toast(message) {
  document.querySelectorAll(".toast").forEach((node) => node.remove());
  const node = el("div", { class: "toast", role: "status", text: message });
  document.body.appendChild(node);
  setTimeout(() => node.remove(), 1600);
}

function normalizeDemo(project) {
  const links = project.links || {};
  const namespace = project.temporalNamespace || "";
  return {
    id: project.name,
    name: project.title || project.name,
    summary: project.summary || "",
    canonical: !!project.canonical,
    tags: Array.isArray(project.tags) ? project.tags : [],
    sdks: Array.isArray(project.temporalSdks) ? project.temporalSdks : [],
    aiSdks: Array.isArray(project.aiSdks) ? project.aiSdks : [],
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
    cloud: {
      namespace: !namespace
        ? ""
        : namespace.includes("a2dd6")
        ? namespace
        : `${namespace}.a2dd6`,
    },
    authRequired: !!project.authRequired,
    crashable: !!(project.crashable && project.crashable.enabled),
    isolatedLambda: !!(project.isolatedLambda && project.isolatedLambda.enabled),
  };
}

// The operator stamps every managed Temporal Cloud namespace with
// owner=<baseDomain> (resources.namespace_tags), coerced to Cloud's tag charset.
function ownerTagValue(baseDomain) {
  return baseDomain.trim().toLowerCase().replace(/[^a-z0-9._@-]/g, "_").slice(0, 63);
}

// Points the header's "See All Namespaces" button at the Cloud namespace list
// filtered to our owner tag (the list page reads `?tags=key:value,...`).
function renderNamespacesLink(baseDomain) {
  const link = document.querySelector("[data-namespaces-link]");
  if (!link || !baseDomain) return;
  const tag = `owner:${ownerTagValue(baseDomain)}`;
  const url = new URL("https://cloud.temporal.io/namespaces");
  url.searchParams.set("tags", tag);
  link.href = url.toString();
  link.title = `Temporal Cloud namespaces tagged ${tag}`;
}

function namespaceUrl(demo) {
  return demo.cloud.namespace
    ? `https://cloud.temporal.io/namespaces/${demo.cloud.namespace}/workflows`
    : null;
}

async function loadData() {
  try {
    const res = await fetch("/api/registry", { credentials: "same-origin" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    const projects = Array.isArray(payload.projects) ? payload.projects : [];
    state.demos = projects
      .map(normalizeDemo)
      .sort((a, b) => a.name.localeCompare(b.name));
    state.courses = Array.isArray(payload.courses) ? payload.courses : [];
    renderNamespacesLink(payload.baseDomain);
  } catch (err) {
    document
      .querySelectorAll("[data-canonical-list], [data-other-list], [data-workshops-list]")
      .forEach((list) =>
        list.replaceChildren(
          el("p", { class: "empty error", text: `Failed to load catalog: ${err.message}` })
        )
      );
    throw err;
  }
}

// ─── Routing ───────────────────────────────────────────────────────────────

function currentView() {
  if (state.route === ONBOARD_ROUTE) return "onboard";
  return state.route === WORKSHOPS_ROUTE ? "workshops" : "demos";
}

function setRoute(target) {
  if (target === state.route) return;
  closeDrawer({ updateHash: false });
  state.route = target;
  const search =
    target === WORKSHOPS_ROUTE
      ? workshopsQueryString()
      : target === ONBOARD_ROUTE
        ? ""
        : demosQueryString();
  window.history.pushState(null, "", target + search);
  render();
  window.scrollTo({ top: 0 });
}

function setupTabs() {
  document.querySelectorAll("[data-route]").forEach((link) => {
    link.addEventListener("click", (event) => {
      if (event.metaKey || event.ctrlKey || event.shiftKey) return;
      event.preventDefault();
      setRoute(link.getAttribute("data-route"));
    });
  });
  window.addEventListener("popstate", () => {
    state.route = window.location.pathname;
    loadFiltersFromUrl();
    render();
    syncDrawerToHash();
  });
  window.addEventListener("hashchange", syncDrawerToHash);
}

function setupKeyboard() {
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && state.openDrawerId) {
      closeDrawer();
      return;
    }
    const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName);
    if (event.key === "/" && !typing && !state.openDrawerId && currentView() !== "onboard") {
      event.preventDefault();
      const selector =
        currentView() === "workshops" ? "[data-workshops-search]" : "[data-demos-search]";
      document.querySelector(selector).focus();
    }
  });
}

// ─── Filters (URL-synced) ──────────────────────────────────────────────────

function splitParam(params, key) {
  return new Set((params.get(key) || "").split(",").filter(Boolean));
}

function loadFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);
  if (currentView() === "workshops") {
    state.workshopsSearch = params.get("q") || "";
    state.selectedWorkshopSdks = splitParam(params, "sdks");
    state.selectedWorkshopAiSdks = splitParam(params, "ai");
    const hours = Number(params.get("hours"));
    state.workshopHours = hours > 0 ? hours : null;
  } else {
    state.demosSearch = params.get("q") || "";
    state.selectedTags = splitParam(params, "tags");
    state.selectedSdks = splitParam(params, "sdks");
    state.selectedDemoAiSdks = splitParam(params, "ai");
  }
}

function buildQuery(entries) {
  const params = new URLSearchParams();
  for (const [key, value] of entries) {
    if (value) params.set(key, value);
  }
  const q = params.toString();
  return q ? `?${q}` : "";
}

function demosQueryString() {
  return buildQuery([
    ["q", state.demosSearch.trim()],
    ["tags", [...state.selectedTags].join(",")],
    ["sdks", [...state.selectedSdks].join(",")],
    ["ai", [...state.selectedDemoAiSdks].join(",")],
  ]);
}

function workshopsQueryString() {
  return buildQuery([
    ["q", state.workshopsSearch.trim()],
    ["sdks", [...state.selectedWorkshopSdks].join(",")],
    ["ai", [...state.selectedWorkshopAiSdks].join(",")],
    ["hours", state.workshopHours ? String(state.workshopHours) : ""],
  ]);
}

function syncFiltersToUrl() {
  const search = currentView() === "workshops" ? workshopsQueryString() : demosQueryString();
  window.history.replaceState(
    null,
    "",
    window.location.pathname + search + window.location.hash
  );
}

function countBy(items, pick) {
  const counts = new Map();
  for (const item of items) {
    for (const value of pick(item)) counts.set(value, (counts.get(value) || 0) + 1);
  }
  return counts;
}

function sdkChip(value, selected, onToggle) {
  return el(
    "button",
    {
      type: "button",
      class: "sdk-toggle",
      style: sdkStyle(value),
      "aria-pressed": selected.has(value) ? "true" : "false",
      onclick: () => {
        if (selected.has(value)) selected.delete(value);
        else selected.add(value);
        onToggle();
      },
    },
    sdkMark(value),
    value
  );
}

// ─── Demos view ────────────────────────────────────────────────────────────

function demosHaveFilters() {
  return !!(
    state.ziggyPicks ||
    state.selectedTags.size ||
    state.selectedSdks.size ||
    state.selectedDemoAiSdks.size ||
    state.demosSearch.trim()
  );
}

function onDemosFilterChange() {
  syncFiltersToUrl();
  renderDemosFilters();
  renderDemosList();
}

function setupDemosFilters() {
  const search = document.querySelector("[data-demos-search]");
  search.addEventListener("input", (event) => {
    state.demosSearch = event.target.value;
    syncFiltersToUrl();
    renderDemosFilters();
    renderDemosList();
  });
  document.querySelector("[data-clear-demos]").addEventListener("click", () => {
    state.demosSearch = "";
    state.selectedTags.clear();
    state.selectedSdks.clear();
    state.selectedDemoAiSdks.clear();
    clearZiggyView();
    onDemosFilterChange();
  });
  // The AI SDK and Topics panels share a spot under the filter bar, so only
  // one stays open, and a click anywhere else closes it.
  const menus = [
    document.querySelector("[data-demos-ai-sdks-menu]"),
    document.querySelector("[data-topics-menu]"),
  ];
  for (const menu of menus) {
    menu.addEventListener("toggle", () => {
      if (!menu.open) return;
      for (const other of menus) if (other !== menu) other.open = false;
    });
  }
  document.addEventListener("click", (event) => {
    for (const menu of menus) {
      if (menu.open && !menu.contains(event.target)) menu.open = false;
    }
  });
}

function renderDemosFilters() {
  document.querySelector("[data-demos-search]").value = state.demosSearch;

  const sdks = [...countBy(state.demos, (d) => d.sdks).keys()].sort();
  document
    .querySelector("[data-demos-sdks]")
    .replaceChildren(...sdks.map((sdk) => sdkChip(sdk, state.selectedSdks, onDemosFilterChange)));
  renderAiSdkMenuFor({
    menu: "[data-demos-ai-sdks-menu]",
    panel: "[data-demos-ai-sdks-panel]",
    count: "[data-demos-ai-sdks-count]",
    name: "demo-ai-sdk",
    items: state.demos,
    aiSdksOf: (demo) => demo.aiSdks,
    countOf: (aiSdk) => state.demos.filter((demo) => demo.aiSdks.includes(aiSdk)).length,
    selected: state.selectedDemoAiSdks,
    onChange: onDemosFilterChange,
  });

  // Rebuilding the panel would lose scroll position mid-interaction, so only
  // build it once and keep checkbox state in sync afterwards.
  const panel = document.querySelector("[data-topics-panel]");
  if (!panel.childElementCount) {
    const tagCounts = countBy(state.demos, (d) => d.tags);
    const tags = [...tagCounts.keys()].sort((a, b) => a.localeCompare(b));
    panel.replaceChildren(
      ...tags.map((tag) =>
        el(
          "label",
          {},
          el("input", {
            type: "checkbox",
            value: tag,
            onchange: (event) => {
              if (event.target.checked) state.selectedTags.add(tag);
              else state.selectedTags.delete(tag);
              onDemosFilterChange();
            },
          }),
          el("span", { text: tag }),
          el("small", { text: String(tagCounts.get(tag)) })
        )
      )
    );
  }
  panel.querySelectorAll("input").forEach((input) => {
    input.checked = state.selectedTags.has(input.value);
  });
  const topicsCount = document.querySelector("[data-topics-count]");
  topicsCount.hidden = !state.selectedTags.size;
  topicsCount.textContent = String(state.selectedTags.size);

  document.querySelector("[data-clear-demos]").hidden = !demosHaveFilters();
  renderZiggyChip();
}

function demoMatchesFilters(demo) {
  if (state.ziggyPicks && !state.ziggyPicks.includes(demo.id)) return false;
  const q = state.demosSearch.trim().toLowerCase();
  const matchesTags =
    state.selectedTags.size === 0 ||
    [...state.selectedTags].every((t) => demo.tags.includes(t));
  const matchesSdks =
    state.selectedSdks.size === 0 ||
    [...state.selectedSdks].some((s) => demo.sdks.includes(s));
  const matchesAiSdks =
    state.selectedDemoAiSdks.size === 0 ||
    [...state.selectedDemoAiSdks].some((s) => demo.aiSdks.includes(s));
  const matchesSearch =
    !q ||
    demo.name.toLowerCase().includes(q) ||
    demo.id.toLowerCase().includes(q) ||
    demo.summary.toLowerCase().includes(q) ||
    demo.tags.some((t) => t.toLowerCase().includes(q)) ||
    demo.sdks.some((s) => s.toLowerCase().includes(q)) ||
    demo.aiSdks.some((s) => s.toLowerCase().includes(q));
  return matchesTags && matchesSdks && matchesAiSdks && matchesSearch;
}

function renderDemosList() {
  const canonical = state.demos.filter((demo) => demo.canonical);
  const others = state.demos.filter((demo) => !demo.canonical);
  // Ziggy lists its picks best first; keep that order within each section.
  const byPick = (a, b) =>
    state.ziggyPicks ? state.ziggyPicks.indexOf(a.id) - state.ziggyPicks.indexOf(b.id) : 0;
  const visibleCanonical = canonical.filter(demoMatchesFilters).sort(byPick);
  const visibleOthers = others.filter(demoMatchesFilters).sort(byPick);

  document.querySelector("[data-canonical-total]").textContent = plural(canonical.length, "demo");
  document.querySelector("[data-other-total]").textContent = plural(others.length, "demo");

  const count = document.querySelector("[data-demos-count]");
  count.hidden = !demosHaveFilters();
  count.textContent = `${visibleCanonical.length + visibleOthers.length} of ${plural(
    state.demos.length,
    "demo"
  )}`;

  const canonicalList = document.querySelector("[data-canonical-list]");
  if (visibleCanonical.length) {
    canonicalList.replaceChildren(...visibleCanonical.map(renderFeatureCard));
  } else {
    canonicalList.replaceChildren(
      el("p", {
        class: "empty",
        text: canonical.length
          ? "No canonical demos match these filters."
          : "No canonical demos yet. Mark a project canonical in its YAML to feature it here.",
      })
    );
  }

  const otherList = document.querySelector("[data-other-list]");
  if (visibleOthers.length) {
    otherList.replaceChildren(...visibleOthers.map(renderDemoCard));
  } else {
    otherList.replaceChildren(
      el("p", {
        class: "empty",
        text: others.length
          ? "No other demos match these filters."
          : "Every registered demo is currently canonical.",
      })
    );
  }
}

function flagTags(demo) {
  return [
    demo.authRequired
      ? el(
          "span",
          {
            class: "tag tag-solid",
            title: "Requires signing in with a @temporal.io email",
          },
          icon("lock"),
          "Sign-in"
        )
      : null,
    demo.crashable
      ? el(
          "span",
          {
            class: "tag tag-hot",
            title: "Supports a private crashable environment",
          },
          icon("bolt"),
          "Crashable"
        )
      : null,
    demo.isolatedLambda
      ? el(
          "span",
          {
            class: "tag tag-isolated",
            title: "Supports a private isolated environment",
          },
          icon("stack"),
          "Isolated"
        )
      : null,
  ];
}

function topicTags(tags, limit = Infinity) {
  const shown = tags.slice(0, limit).map((tag) => el("span", { class: "tag", text: tag }));
  if (tags.length > limit) {
    shown.push(
      el("span", {
        class: "tag tag-more",
        title: tags.slice(limit).join(", "),
        text: `+${tags.length - limit}`,
      })
    );
  }
  return shown;
}

function tagRow(tags) {
  const present = tags.filter(Boolean);
  return present.length ? el("div", { class: "tags" }, ...present) : null;
}

function openAppButton(demo, cls = "btn btn-ghost btn-sm", label = "Open app") {
  if (!demo.technicalAssets.appUrl) return null;
  return externalLink(
    demo.technicalAssets.appUrl,
    { class: cls, onclick: (event) => event.stopPropagation() },
    label,
    icon("external")
  );
}

function namespaceButton(demo, cls = "btn btn-ghost btn-sm") {
  const url = namespaceUrl(demo);
  if (!url) return null;
  return externalLink(
    url,
    {
      class: cls,
      title: `Open ${demo.cloud.namespace} in Temporal Cloud`,
      onclick: (event) => event.stopPropagation(),
    },
    "Namespace",
    icon("external")
  );
}

// Deterministic artwork for featured demos in the temporal.io style (color
// glows over a grid, stepped pixel blocks, a sparkle), seeded by the demo id
// so each demo keeps its own composition across reloads.
function seededRandom(seed) {
  let h = 2166136261;
  for (const ch of seed) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
  return () => {
    h ^= h << 13;
    h ^= h >>> 17;
    h ^= h << 5;
    return (h >>> 0) / 4294967296;
  };
}

function demoArt(demo) {
  const rand = seededRandom(demo.id || "demo");
  const pick = (list) => list[Math.floor(rand() * list.length)];
  const pct = (min, max) => `${Math.round(min + rand() * (max - min))}%`;
  const glows = [
    "rgb(255 0 223 / .55)",
    "rgb(68 76 231 / .85)",
    "rgb(89 253 160 / .4)",
    "rgb(182 100 255 / .65)",
  ];
  const blocks = ["#f8fafc", "#fecb2f", "#59fda0", "#ff6bff", "#f55", "var(--grad-uv)", "var(--grad-pink)"];
  const glow1 = pick(glows);
  const glow2 = pick(glows.filter((glow) => glow !== glow1));
  const cluster = (x, y) =>
    el(
      "span",
      { class: "art-cluster", style: `--x:${x};--y:${y}` },
      el("i", { style: `--w:${32 * (1 + Math.floor(rand() * 2))}px;--c:${pick(blocks)}` }),
      el("i", {
        style: `--w:${48 * (1 + Math.floor(rand() * 3))}px;--c:${pick(blocks)};--dy:1.25rem`,
      })
    );
  const art = el(
    "div",
    {
      class: "art",
      "aria-hidden": "true",
      style: `--g1x:${pct(10, 40)};--g1y:${pct(10, 40)};--g1c:${glow1};--g2x:${pct(60, 95)};--g2y:${pct(55, 95)};--g2c:${glow2}`,
    },
    cluster(pct(6, 28), pct(8, 20)),
    cluster(pct(38, 58), pct(56, 66)),
    sparkle(`--x:${pct(58, 80)};--y:${pct(14, 32)};--size:${(2.5 + rand() * 1.5).toFixed(2)}rem`),
    el("div", { class: "art-label" }, ...demo.sdks.map(sdkMark), el("span", { text: demo.id }))
  );
  art.appendChild(screenshotFrame(demo, art));
  return art;
}

// A screenshot dropped at static/screenshots/<demo id>.jpg is shown in a
// browser-window frame over the art. Until it loads (or if there is none) the
// frame stays invisible and the generated art stands on its own.
function screenshotFrame(demo, art) {
  const appUrl = demo.technicalAssets.appUrl;
  let host = demo.id;
  try {
    if (appUrl) host = new URL(appUrl).host;
  } catch (err) {
    // keep the demo id as the label
  }
  const img = el("img", {
    src: `/static/screenshots/${encodeURIComponent(demo.id)}.jpg`,
    alt: "",
    loading: "lazy",
    decoding: "async",
  });
  const frame = el(
    appUrl ? "a" : "div",
    appUrl
      ? { class: "art-shot", href: appUrl, target: "_blank", rel: "noopener noreferrer", tabindex: "-1" }
      : { class: "art-shot" },
    el("div", { class: "art-shot-bar" }, el("i"), el("i"), el("i"), el("span", { text: host })),
    img
  );
  img.addEventListener("load", () => art.classList.add("has-shot"));
  img.addEventListener("error", () => frame.remove());
  return frame;
}

// [full label, short card label, url] for each presenter asset. A null short
// label keeps the asset off catalog cards: every demo has a repo (the
// DemoProject points at it), so a Repo chip would say nothing.
function presenterAssets(demo) {
  const nt = demo.nonTechnicalAssets;
  return [
    ["Speaker notes", "Notes", nt.speakerNotes],
    ["Slide deck", "Deck", nt.slideDeck],
    ["Golden path recording", "Golden path", nt.goldenPath],
    ["External recording", "Recording", nt.externalRecording],
    ["Runbook", "Runbook", demo.technicalAssets.runbook],
    ["Repository", null, demo.technicalAssets.repo],
  ];
}

// Catalog and workshop cards give every asset a fixed slot so each one sits in
// the same place on every card; missing assets stay as dimmed placeholders.
function cardAssets(assets) {
  return el(
    "div",
    { class: "card-assets", role: "list", "aria-label": "Presenter assets" },
    ...assets
      .filter(([, short]) => short)
      .map(([label, short, url]) =>
        url
          ? externalLink(url, { role: "listitem", title: label }, short)
          : el("span", { role: "listitem", title: `${label}: not provided yet`, text: short })
      )
  );
}

function renderFeatureCard(demo) {
  const nt = demo.nonTechnicalAssets;
  return el(
    "article",
    {
      class: ziggyPickClass("feature", demo),
      id: `demo-${demo.id}`,
      // The whole tile opens the drawer, like the catalog cards. Links and
      // buttons inside it (actions, checklist, screenshot) keep their own target.
      onclick: (event) => {
        if (event.target.closest("a, button")) return;
        if (window.getSelection()?.toString()) return;
        openDrawer(demo.id);
      },
    },
    demoArt(demo),
    el(
      "div",
      { class: "feature-body" },
      el("p", { class: "eyebrow", text: "Canonical demo" }),
      el(
        "h3",
        {},
        el("button", {
          type: "button",
          class: "card-open",
          text: demo.name,
          onclick: () => openDrawer(demo.id),
        })
      ),
      el("p", { class: "feature-summary", text: demo.summary }),
      sdkList(demo.sdks, demo.aiSdks),
      tagRow([...ziggyTag(demo), ...flagTags(demo), ...topicTags(demo.tags)]),
      el(
        "div",
        { class: "feature-actions" },
        openAppButton(demo, "btn btn-primary", "Open demo") ||
          el("span", { class: "btn btn-primary", "aria-disabled": "true", text: "No app URL" }),
        nt.speakerNotes
          ? externalLink(nt.speakerNotes, { class: "btn btn-secondary" }, "Speaker notes")
          : null,
        namespaceButton(demo, "btn btn-ghost")
      ),
      el(
        "div",
        { class: "checklist", role: "list", "aria-label": "Presenter assets" },
        ...presenterAssets(demo).map(([label, , url]) =>
          url
            ? externalLink(url, { role: "listitem" }, label)
            : el("span", { role: "listitem", title: "Not provided yet", text: label })
        )
      )
    )
  );
}

function renderDemoCard(demo) {
  return el(
    "article",
    { class: ziggyPickClass("card", demo), id: `demo-${demo.id}`, style: `--sdk-bar:${sdkBar(demo.sdks)}` },
    el(
      "h3",
      {},
      el("button", {
        type: "button",
        class: "card-open",
        text: demo.name,
        onclick: () => openDrawer(demo.id),
      })
    ),
    demo.summary ? el("p", { class: "card-summary", text: demo.summary }) : null,
    tagRow([...ziggyTag(demo), ...flagTags(demo), ...topicTags(demo.tags, 3)]),
    el("div", { class: "card-foot" }, cardAssets(presenterAssets(demo)), sdkList(demo.sdks, demo.aiSdks), cardActions(demo))
  );
}

function cardActions(demo) {
  const actions = [openAppButton(demo), namespaceButton(demo)].filter(Boolean);
  return actions.length ? el("div", { class: "card-actions" }, ...actions) : null;
}

// ─── Detail drawer ─────────────────────────────────────────────────────────
// Demos and workshops share one drawer, deep-linked by hash on their own page
// (/#<demo id>, /courses#<workshop key>).

let drawerReturnFocus = null;

function findDemo(id) {
  return state.demos.find((demo) => demo.id === id) || null;
}

// The drawer content for `id` on the current page, or null if there is none.
function drawerContentFor(id) {
  if (currentView() === "workshops") {
    const members = findWorkshop(id);
    return members ? renderWorkshopDrawerContent(id, members) : null;
  }
  if (currentView() !== "demos") return null;
  const demo = findDemo(id);
  return demo ? renderDrawerContent(demo) : null;
}

function drawerTargetExists(id) {
  if (currentView() === "workshops") return Boolean(findWorkshop(id));
  return currentView() === "demos" && Boolean(findDemo(id));
}

function openDrawer(id, { updateHash = true } = {}) {
  const content = drawerContentFor(id);
  if (!content) return;
  if (!state.openDrawerId) drawerReturnFocus = document.activeElement;
  state.openDrawerId = id;
  if (updateHash && window.location.hash !== `#${id}`) {
    window.history.pushState(null, "", `${window.location.pathname}${window.location.search}#${id}`);
  }
  const drawer = document.querySelector("[data-drawer]");
  const panel = document.querySelector("[data-drawer-panel]");
  clearAllWorkspaceTimers();
  panel.replaceChildren(...content);
  panel.scrollTop = 0;
  drawer.hidden = false;
  document.body.classList.add("drawer-open");
  panel.focus();
}

function closeDrawer({ updateHash = true } = {}) {
  if (!state.openDrawerId) return;
  state.openDrawerId = null;
  clearAllWorkspaceTimers();
  document.querySelector("[data-drawer]").hidden = true;
  document.querySelector("[data-drawer-panel]").replaceChildren();
  document.body.classList.remove("drawer-open");
  if (updateHash && window.location.hash) {
    window.history.pushState(null, "", window.location.pathname + window.location.search);
  }
  if (drawerReturnFocus && document.contains(drawerReturnFocus)) drawerReturnFocus.focus();
  drawerReturnFocus = null;
}

function syncDrawerToHash() {
  const id = window.location.hash.slice(1);
  if (id && drawerTargetExists(id)) {
    if (state.openDrawerId !== id) openDrawer(id, { updateHash: false });
  } else {
    closeDrawer({ updateHash: false });
  }
}

function setupDrawer() {
  document.querySelector("[data-drawer-close]").addEventListener("click", () => closeDrawer());
}

function copyLink(id) {
  const path = currentView() === "workshops" ? WORKSHOPS_ROUTE : "/";
  const url = `${window.location.origin}${path}#${id}`;
  if (navigator.clipboard) navigator.clipboard.writeText(url).catch(() => {});
  toast("Link copied");
}

function launchTile(label, url, text, { primary = false } = {}) {
  if (!url) {
    return el(
      "div",
      { class: "launch is-missing" },
      el("small", { text: label }),
      el("strong", { text: "Not available" })
    );
  }
  return externalLink(
    url,
    { class: `launch${primary ? " is-primary" : ""}` },
    el("small", { text: label }),
    el("strong", {}, text, icon("external"))
  );
}

function assetRow(label, url) {
  if (!url) {
    return el(
      "div",
      { class: "asset-row is-missing" },
      el("span", { text: label }),
      el("em", { text: "Not provided" })
    );
  }
  return el(
    "div",
    { class: "asset-row" },
    el("span", { title: label, text: label }),
    externalLink(url, {}, `${linkLabel(url)} ↗`)
  );
}

function renderDrawerContent(demo) {
  const technicalAdditional = demo.additional.filter((a) => a.audience === "technical");
  const nonTechnicalAdditional = demo.additional.filter((a) => a.audience === "nonTechnical");

  const head = el(
    "div",
    { class: "drawer-head" },
    el("p", {
      class: demo.canonical ? "eyebrow" : "eyebrow is-quiet",
      text: demo.canonical ? "Canonical demo" : "Demo",
    }),
    el(
      "div",
      { class: "drawer-head-actions" },
      el(
        "button",
        { type: "button", class: "btn btn-sm btn-quiet", onclick: () => copyLink(demo.id) },
        icon("link"),
        "Copy link"
      ),
      el(
        "button",
        {
          type: "button",
          class: "icon-btn",
          "aria-label": "Close",
          onclick: () => closeDrawer(),
        },
        icon("close")
      )
    )
  );

  const title = el(
    "div",
    { class: "drawer-title" },
    el("h2", { id: "drawer-title", text: demo.name }),
    el("p", { text: demo.summary }),
    sdkList(demo.sdks, demo.aiSdks),
    tagRow(flagTags(demo))
  );

  const launch = el(
    "div",
    { class: "launch-grid" },
    launchTile("App", demo.technicalAssets.appUrl, "Open demo", { primary: true }),
    launchTile("Temporal Cloud", namespaceUrl(demo), "Namespace"),
    launchTile("Presenting", demo.nonTechnicalAssets.speakerNotes, "Speaker notes")
  );

  const assets = el(
    "div",
    { class: "asset-cols" },
    el(
      "div",
      { class: "panel" },
      el("h3", { class: "panel-title", text: "Technical" }),
      el(
        "div",
        { class: "asset-list" },
        assetRow("Repository", demo.technicalAssets.repo),
        assetRow("Runbook", demo.technicalAssets.runbook),
        ...technicalAdditional.map((a) => assetRow(a.label, a.url))
      )
    ),
    el(
      "div",
      { class: "panel" },
      el("h3", { class: "panel-title", text: "Presenting" }),
      el(
        "div",
        { class: "asset-list" },
        assetRow("Slide deck", demo.nonTechnicalAssets.slideDeck),
        assetRow("Golden path recording", demo.nonTechnicalAssets.goldenPath),
        assetRow("External recording", demo.nonTechnicalAssets.externalRecording),
        ...nonTechnicalAdditional.map((a) => assetRow(a.label, a.url))
      )
    )
  );

  const meta = el(
    "div",
    { class: "panel" },
    el("h3", { class: "panel-title", text: "Topics" }),
    tagRow(topicTags(demo.tags)) || el("p", { class: "ws-note", text: "No topics tagged." }),
    demo.cloud.namespace
      ? el(
          "p",
          { class: "mono", style: "margin-top:.9rem" },
          `namespace: ${demo.cloud.namespace}`
        )
      : null
  );

  return [
    head,
    el(
      "div",
      { class: "drawer-body" },
      title,
      launch,
      demo.isolatedLambda
        ? workspaceCard(demo, isolatedWorkspacePresentation())
        : demo.crashable
          ? workspaceCard(demo, crashableWorkspacePresentation())
          : null,
      assets,
      meta
    ),
  ];
}

// ─── Crashable workspace ───────────────────────────────────────────────────
// A per-(user, demo) ephemeral clone of the demo's Kubernetes namespace, driven
// by a Temporal entity workflow. The card renders inside the demo drawer from a
// client-side cache of the workflow's status query and re-arms its own
// poll/countdown timers on each render. Timers are keyed by demo id and cleared
// whenever the drawer opens, closes, or switches demo; each re-rendered card
// looks itself up in the DOM via [data-workspace=<id>].

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
  return document.querySelector(`[data-drawer-panel] [data-workspace="${id}"]`);
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

function workspaceButton(label, cls, onclick) {
  return el(
    "button",
    {
      type: "button",
      class: `btn btn-sm ${cls}`,
      onclick: (event) => {
        event.preventDefault();
        event.stopPropagation();
        event.currentTarget.setAttribute("aria-disabled", "true");
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

// Mints a guest link for the running workspace (auth-gated demos only). The
// link works only on this workspace's host and stops working when it expires,
// links are revoked, or the environment is deleted.
async function copyWorkspaceShareLink(demo) {
  try {
    const { url } = await postJSON("/api/crashable-workspace/share", { demo: demo.id });
    try {
      await navigator.clipboard.writeText(url);
      toast("Share link copied — valid for 4 hours");
    } catch (err) {
      window.prompt("Copy this share link (valid for 4 hours):", url);
    }
  } catch (err) {
    toast(`Could not create share link: ${err.message}`);
  }
  const node = workspaceContainer(demo.id);
  if (node) renderWorkspaceInto(demo, node);
}

async function revokeWorkspaceShareLinks(demo) {
  try {
    const status = await postJSON("/api/crashable-workspace/revoke-share-links", {
      demo: demo.id,
    });
    workspaceState.set(demo.id, status);
    toast("All share links revoked");
  } catch (err) {
    toast(`Could not revoke share links: ${err.message}`);
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

function crashableWorkspacePresentation() {
  return {
    mode: "crashable",
    title: "Crashable environment",
    panelClass: "panel panel-crash",
    emptyNote:
      "Spin up a private, time-limited copy of this demo’s Kubernetes workers that you can crash or scale down to watch Temporal recover. Auto-deletes after 1 hour.",
    createLabel: "Create crashable environment",
  };
}

function isolatedWorkspacePresentation() {
  return {
    mode: "isolated-lambda",
    title: "Isolated environment",
    panelClass: "panel panel-isolated",
    emptyNote:
      "Spin up a private, time-limited copy of this demo. Your traffic and deployments stay on their own workers. Auto-deletes after 1 hour.",
    createLabel: "Create isolated environment",
  };
}

function workspaceCard(demo, presentation) {
  const container = el("section", {
    class: presentation.panelClass,
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

function workspaceBusy(text, step) {
  return el(
    "div",
    { class: "ws-status" },
    el("span", { class: "spinner", "aria-hidden": "true" }),
    el(
      "div",
      {},
      el("strong", { dataset: step !== undefined ? {} : { workspaceStep: "" }, text }),
      step !== undefined
        ? el("small", { dataset: { workspaceStep: "" }, text: step })
        : null
    )
  );
}

function workspaceCountdown(expiresAt) {
  return el(
    "p",
    { class: "ws-meta" },
    "Auto-deletes in ",
    el("b", {
      dataset: { workspaceCountdown: "" },
      text: expiresAt ? formatCountdown(expiresAt - Date.now()) : "—",
    })
  );
}

function renderWorkspaceInto(demo, container) {
  const presentation = demo.isolatedLambda
    ? isolatedWorkspacePresentation()
    : crashableWorkspacePresentation();
  clearWorkspaceTimers(demo.id);
  const status = workspaceState.get(demo.id) || null;
  const phase = status
    ? status.phase
    : workspaceFetching.has(demo.id)
    ? "Loading"
    : "None";
  const isolated = presentation.mode === "isolated-lambda";

  const children = [el("h3", { class: "panel-title", text: presentation.title })];
  const note = (text) => el("p", { class: "ws-note", text });
  const expiresAt = status && status.expires_at ? Date.parse(status.expires_at) : null;
  const trailer = () => [
    workspaceCountdown(expiresAt),
    status && status.namespace ? el("p", { class: "mono", text: status.namespace }) : null,
    status && status.last_error ? el("p", { class: "ws-error", text: status.last_error }) : null,
  ];
  const extend = () =>
    workspaceButton("Extend +1h", "btn-secondary", () =>
      workspaceAction(demo, "/api/crashable-workspace/extend")
    );
  const remove = () =>
    workspaceButton("Delete", "btn-quiet", () =>
      workspaceAction(demo, "/api/crashable-workspace/delete")
    );
  // Only auth-gated demos need a share link; public ones are already open.
  const sharing = () =>
    demo.authRequired
      ? el(
          "div",
          { class: "ws-actions" },
          workspaceButton("Copy share link", "btn-secondary", () =>
            copyWorkspaceShareLink(demo)
          ),
          workspaceButton("Revoke links", "btn-quiet", () =>
            revokeWorkspaceShareLinks(demo)
          )
        )
      : null;

  if (phase === "Loading") {
    children.push(note("Checking for an existing environment…"));
  } else if (phase === "None") {
    children.push(
      note(presentation.emptyNote),
      el(
        "div",
        { class: "ws-actions" },
        workspaceButton(presentation.createLabel, "btn-primary", () =>
          workspaceAction(demo, "/api/crashable-workspace")
        )
      )
    );
  } else if (phase === "Provisioning") {
    children.push(
      workspaceBusy("Creating your environment…", (status && status.step) || "Starting…")
    );
    setWorkspaceTimer(demo.id, armWorkspacePolling(demo));
  } else if (phase === "Crashing" || phase === "ScalingDown" || phase === "ScalingUp") {
    children.push(
      workspaceBusy(
        (status && status.step) ||
          (phase === "ScalingDown"
            ? "Scaling workers to zero…"
            : phase === "ScalingUp"
            ? "Scaling workers to one…"
            : "Crashing workers…")
      )
    );
    setWorkspaceTimer(demo.id, armWorkspacePolling(demo));
  } else if (phase === "Deleting") {
    children.push(workspaceBusy("Deleting environment…"));
    setWorkspaceTimer(demo.id, armWorkspacePolling(demo));
  } else if (phase === "Failed") {
    children.push(
      el("p", {
        class: "ws-error",
        text: (status && status.last_error) || "Environment provisioning failed.",
      }),
      el(
        "div",
        { class: "ws-actions" },
        workspaceButton("Try again", "btn-primary", () =>
          workspaceAction(demo, "/api/crashable-workspace")
        ),
        workspaceButton("Dismiss", "btn-quiet", () =>
          workspaceAction(demo, "/api/crashable-workspace/delete")
        )
      )
    );
  } else if (phase === "Active") {
    const appUrl = (status && status.app_url) || null;
    const open = appUrl
      ? externalLink(appUrl, { class: "btn btn-sm btn-primary" }, "Open environment", icon("external"))
      : null;
    const runningActions = isolated
      ? [open, extend(), remove()]
      : [
          open,
          workspaceButton("Crash workers", "btn-pink", () =>
            workspaceAction(demo, "/api/crashable-workspace/crash")
          ),
          workspaceButton("Scale down workers", "btn-secondary", () =>
            workspaceAction(demo, "/api/crashable-workspace/scale-down")
          ),
          extend(),
          remove(),
        ];
    children.push(
      el("span", { class: "ws-live", text: "Running" }),
      el(
        "div",
        { class: "ws-actions", style: "margin-top:0" },
        ...runningActions
      ),
      sharing(),
      ...trailer()
    );
    setWorkspaceTimer(demo.id, armWorkspaceCountdown(demo, expiresAt));
  } else if (phase === "ScaledDown" && !isolated) {
    children.push(
      el("span", { class: "ws-live is-paused", text: "Workers scaled to zero" }),
      note(
        "Application pods remain online while Temporal work waits durably for the workers to return."
      ),
      el(
        "div",
        { class: "ws-actions" },
        workspaceButton("Scale up workers", "btn-green", () =>
          workspaceAction(demo, "/api/crashable-workspace/scale-up")
        ),
        extend(),
        remove()
      ),
      sharing(),
      ...trailer()
    );
    setWorkspaceTimer(demo.id, armWorkspaceCountdown(demo, expiresAt));
  }

  if (status && status.lastActionError) {
    children.push(el("p", { class: "ws-error", text: status.lastActionError }));
  }

  container.replaceChildren(...children.filter(Boolean));
}

// ─── Workshops view ────────────────────────────────────────────────────────

function workshopsHaveFilters() {
  return !!(
    state.selectedWorkshopSdks.size ||
    state.selectedWorkshopAiSdks.size ||
    state.workshopHours ||
    state.workshopsSearch.trim()
  );
}

function onWorkshopsFilterChange() {
  syncFiltersToUrl();
  renderWorkshopsFilters();
  renderWorkshopsList();
}

function setupWorkshopsFilters() {
  document.querySelector("[data-workshops-search]").addEventListener("input", (event) => {
    state.workshopsSearch = event.target.value;
    syncFiltersToUrl();
    renderWorkshopsFilters();
    renderWorkshopsList();
  });
  document.querySelector("[data-clear-workshops]").addEventListener("click", () => {
    state.workshopsSearch = "";
    state.selectedWorkshopSdks.clear();
    state.selectedWorkshopAiSdks.clear();
    state.workshopHours = null;
    onWorkshopsFilterChange();
  });
  // The AI SDK and length panels share a spot under the filter bar, so only
  // one stays open, and a click anywhere else closes it.
  const menus = [
    document.querySelector("[data-ai-sdks-menu]"),
    document.querySelector("[data-duration-menu]"),
  ];
  for (const menu of menus) {
    menu.addEventListener("toggle", () => {
      if (!menu.open) return;
      for (const other of menus) if (other !== menu) other.open = false;
    });
  }
  document.addEventListener("click", (event) => {
    for (const menu of menus) {
      if (menu.open && !menu.contains(event.target)) menu.open = false;
    }
  });
}

function courseSdks(course) {
  return Array.isArray(course.temporalSdks) ? course.temporalSdks : [];
}

function courseAiSdks(course) {
  return Array.isArray(course.aiSdks) ? course.aiSdks : [];
}

// A track's supported session length as [minHours, maxHours], or null.
function courseDuration(course) {
  const min = Number(course.duration?.minHours);
  const max = Number(course.duration?.maxHours);
  return min > 0 && max >= min ? [min, max] : null;
}

function courseFitsHours(course, hours) {
  const range = courseDuration(course);
  return !!range && range[0] <= hours && hours <= range[1];
}

function formatHours(hours) {
  return `${hours} ${hours === 1 ? "hour" : "hours"}`;
}

function durationLabel([min, max]) {
  return min === max ? formatHours(max) : `${min}–${formatHours(max)}`;
}

// Session lengths worth offering in the Length menu: every whole hour across
// the catalog's duration ranges plus each range's own ends, kept only when at
// least one track fits.
function sessionLengthOptions() {
  const ranges = state.courses.map(courseDuration).filter(Boolean);
  if (!ranges.length) return [];
  const values = new Set(ranges.flat());
  const low = Math.max(1, Math.floor(Math.min(...ranges.map(([min]) => min))));
  const high = Math.ceil(Math.max(...ranges.map(([, max]) => max)));
  for (let hours = low; hours <= high; hours += 1) values.add(hours);
  return [...values]
    .filter((hours) => ranges.some(([min, max]) => min <= hours && hours <= max))
    .sort((a, b) => a - b);
}

function groupCount(predicate) {
  return groupCourses(state.courses).filter((members) => members.some(predicate)).length;
}

function menuOption(type, name, value, label, count, checked, onChange) {
  return el(
    "label",
    {},
    el("input", { type, name, value, checked, onchange: onChange }),
    el("span", { text: label }),
    count === null ? null : el("small", { text: String(count) })
  );
}

function setMenuCount(selector, count) {
  const badge = document.querySelector(selector);
  badge.hidden = !count;
  badge.textContent = String(count);
}

// Fills an AI SDK checkbox menu from `items` and keeps its checked state in
// step with `selected`.
function renderAiSdkMenuFor({ menu, panel, count, name, items, aiSdksOf, countOf, selected, onChange }) {
  const menuEl = document.querySelector(menu);
  const panelEl = document.querySelector(panel);
  const aiSdks = [...countBy(items, aiSdksOf).keys()].sort((a, b) => a.localeCompare(b));
  menuEl.hidden = !aiSdks.length;
  // Rebuilding the panel mid-click would detach the clicked input and close
  // the menu, so build it once and sync checkbox state afterwards.
  if (!panelEl.childElementCount && aiSdks.length) {
    panelEl.replaceChildren(
      ...aiSdks.map((aiSdk) =>
        menuOption("checkbox", name, aiSdk, aiSdk, countOf(aiSdk), false, (event) => {
          if (event.target.checked) selected.add(aiSdk);
          else selected.delete(aiSdk);
          onChange();
        })
      )
    );
  }
  panelEl.querySelectorAll("input").forEach((input) => {
    input.checked = selected.has(input.value);
  });
  setMenuCount(count, selected.size);
}

function renderAiSdkMenu() {
  renderAiSdkMenuFor({
    menu: "[data-ai-sdks-menu]",
    panel: "[data-ai-sdks-panel]",
    count: "[data-ai-sdks-count]",
    name: "workshop-ai-sdk",
    items: state.courses,
    aiSdksOf: courseAiSdks,
    countOf: (aiSdk) => groupCount((course) => courseAiSdks(course).includes(aiSdk)),
    selected: state.selectedWorkshopAiSdks,
    onChange: onWorkshopsFilterChange,
  });
}

function renderDurationMenu() {
  const menu = document.querySelector("[data-duration-menu]");
  const panel = document.querySelector("[data-duration-panel]");
  const options = sessionLengthOptions();
  menu.hidden = !options.length;
  if (!panel.childElementCount && options.length) {
    const choose = (hours) => () => {
      state.workshopHours = hours;
      onWorkshopsFilterChange();
    };
    panel.replaceChildren(
      menuOption("radio", "workshop-hours", "", "Any length", null, false, choose(null)),
      ...options.map((hours) =>
        menuOption(
          "radio",
          "workshop-hours",
          String(hours),
          `Fits ${formatHours(hours)}`,
          groupCount((course) => courseFitsHours(course, hours)),
          false,
          choose(hours)
        )
      )
    );
  }
  panel.querySelectorAll("input").forEach((input) => {
    input.checked = input.value === (state.workshopHours ? String(state.workshopHours) : "");
  });
  setMenuCount("[data-duration-count]", state.workshopHours ? 1 : 0);
}

function renderWorkshopsFilters() {
  document.querySelector("[data-workshops-search]").value = state.workshopsSearch;
  const sdks = [...countBy(state.courses, courseSdks).keys()].sort();
  document
    .querySelector("[data-workshops-sdks]")
    .replaceChildren(
      ...sdks.map((sdk) => sdkChip(sdk, state.selectedWorkshopSdks, onWorkshopsFilterChange))
    );
  renderAiSdkMenu();
  renderDurationMenu();
  document.querySelector("[data-clear-workshops]").hidden = !workshopsHaveFilters();
}

// A track's row label: its catalog variant, else its SDK; null when it has neither.
function trackLabel(course) {
  if (typeof course.variant === "string" && course.variant.trim()) return course.variant.trim();
  return courseSdks(course)[0] || null;
}

function courseVariantLabel(course) {
  if (typeof course.variant === "string" && course.variant.trim()) return course.variant.trim();
  if (courseSdks(course)[0]) return courseSdks(course)[0];
  return course.title || course.name;
}

// Tracks sharing a catalog `parent` (e.g. one workshop per SDK) collapse into
// a single card whose drawer has one launch tile per variant.
function groupCourses(courses) {
  const groups = new Map();
  for (const course of courses) {
    const key = course.parent || course.name;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(course);
  }
  return [...groups.values()].map((members) =>
    members
      .slice()
      .sort((a, b) => courseVariantLabel(a).localeCompare(courseVariantLabel(b)))
  );
}

function courseHaystack(course) {
  return [
    course.name,
    course.title,
    course.summary,
    course.parent,
    course.variant,
    ...(Array.isArray(course.tags) ? course.tags : []),
    ...courseSdks(course),
    ...courseAiSdks(course),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function courseMatchesSdk(course) {
  return (
    state.selectedWorkshopSdks.size > 0 &&
    courseSdks(course).some((sdk) => state.selectedWorkshopSdks.has(sdk))
  );
}

// SDK, AI SDK and length filters apply per track, so a workshop shows when
// one of its tracks satisfies all of them at once.
function courseMatchesFilters(course) {
  return (
    (!state.selectedWorkshopSdks.size || courseMatchesSdk(course)) &&
    (!state.selectedWorkshopAiSdks.size ||
      courseAiSdks(course).some((aiSdk) => state.selectedWorkshopAiSdks.has(aiSdk))) &&
    (!state.workshopHours || courseFitsHours(course, state.workshopHours))
  );
}

function filteredCourseGroups() {
  const q = state.workshopsSearch.trim().toLowerCase();
  return groupCourses(state.courses).filter(
    (members) =>
      (!q || members.some((course) => courseHaystack(course).includes(q))) &&
      members.some(courseMatchesFilters)
  );
}

function uniqueValues(values) {
  return [...new Set(values.filter(Boolean))];
}

// Extra links listed in the workshop drawer: catalog.resources plus
// catalog.links.additional, once per URL across the group's tracks.
function courseResources(members) {
  const seen = new Set();
  const resources = [];
  for (const course of members) {
    const additional = Array.isArray(course.links?.additional) ? course.links.additional : [];
    for (const resource of [
      ...(Array.isArray(course.resources) ? course.resources : []),
      ...additional,
    ]) {
      if (!resource || !resource.url || seen.has(resource.url)) continue;
      seen.add(resource.url);
      resources.push(resource);
    }
  }
  return resources;
}

// The same presenter-asset slots as demo cards. Language variants of one
// workshop usually share a deck or recording, so each slot takes the first
// track in the group that sets it, starting with the representative.
function workshopAssets(representative, members) {
  const ordered = [representative, ...members.filter((course) => course !== representative)];
  const link = (key) => ordered.map((course) => course.links?.[key]?.url).find(Boolean);
  return [
    ["Speaker notes", "Notes", link("speakerNotes")],
    ["Slide deck", "Deck", link("slideDeck")],
    ["Golden path recording", "Golden path", link("goldenPath")],
    ["External recording", "Recording", link("externalRecording")],
    ["Runbook", "Runbook", link("runbook")],
  ];
}

// A workshop group's key: the catalog `parent` its tracks share, else the lone
// track's name. It is also the workshop's deep link (/courses#<key>).
function workshopKey(members) {
  return members[0].parent || members[0].name;
}

function findWorkshop(key) {
  return groupCourses(state.courses).find((members) => workshopKey(members) === key) || null;
}

// What the card and drawer show for a workshop, merged across its tracks.
function workshopInfo(members) {
  const representative =
    members.find((course) => course.name === course.parent) ||
    members.slice().sort((a, b) => a.name.localeCompare(b.name))[0];
  const ranges = members.map(courseDuration).filter(Boolean);
  return {
    key: workshopKey(members),
    representative,
    title: representative.title || representative.name,
    summary: representative.summary || "A hands-on Temporal workshop.",
    sdks: uniqueValues(members.flatMap(courseSdks)),
    aiSdks: uniqueValues(members.flatMap(courseAiSdks)),
    tags: uniqueValues(members.flatMap((course) => course.tags || [])).filter(
      (tag) => tag.toLowerCase() !== "workshop"
    ),
    duration: ranges.length
      ? durationLabel([
          Math.min(...ranges.map(([min]) => min)),
          Math.max(...ranges.map(([, max]) => max)),
        ])
      : null,
    launchable: members.filter((course) => course.trackUrl),
  };
}

function workshopEyebrow(info) {
  return [
    "Instruqt workshop",
    info.launchable.length ? plural(info.launchable.length, "track") : null,
    info.duration,
  ]
    .filter(Boolean)
    .join(" · ");
}

// Cards stay compact like the demo cards: languages show inline, and the
// tracks themselves are launched from the drawer.
function renderWorkshopCard(members) {
  const info = workshopInfo(members);
  return el(
    "article",
    { class: "workshop", id: `workshop-${info.key}` },
    el("p", { class: "eyebrow", text: workshopEyebrow(info) }),
    el(
      "h3",
      {},
      el("button", {
        type: "button",
        class: "card-open",
        text: info.title,
        onclick: () => openDrawer(info.key),
      })
    ),
    el("p", { class: "workshop-summary", text: info.summary }),
    sdkList(info.sdks, info.aiSdks),
    tagRow(topicTags(info.tags, 3)),
    el("div", { class: "card-foot" }, cardAssets(workshopAssets(info.representative, members)))
  );
}

// One launch tile per published track. Tracks matching the SDK filter (or the
// only track) get the primary treatment.
function trackTile(course, info) {
  const label = trackLabel(course);
  const sdk = courseSdks(course)[0];
  const range = courseDuration(course);
  const primary = info.launchable.length === 1 || courseMatchesSdk(course);
  return externalLink(
    course.trackUrl,
    {
      class: `launch${primary ? " is-primary" : ""}`,
      "aria-label": label
        ? `Open the ${label} track in Instruqt`
        : `Open ${info.title} in Instruqt`,
    },
    el("small", { text: range ? durationLabel(range) : "Instruqt track" }),
    el(
      "strong",
      {},
      el("span", { class: "launch-track" }, sdk ? sdkMark(sdk) : null, label || "Open in Instruqt"),
      icon("external")
    )
  );
}

function renderWorkshopDrawerContent(key, members) {
  const info = workshopInfo(members);
  const assets = workshopAssets(info.representative, members);
  const assetUrl = (label) => assets.find(([full]) => full === label)?.[2];

  const head = el(
    "div",
    { class: "drawer-head" },
    el("p", { class: "eyebrow", text: workshopEyebrow(info) }),
    el(
      "div",
      { class: "drawer-head-actions" },
      el(
        "button",
        { type: "button", class: "btn btn-sm btn-quiet", onclick: () => copyLink(key) },
        icon("link"),
        "Copy link"
      ),
      el(
        "button",
        { type: "button", class: "icon-btn", "aria-label": "Close", onclick: () => closeDrawer() },
        icon("close")
      )
    )
  );

  const title = el(
    "div",
    { class: "drawer-title" },
    el("h2", { id: "drawer-title", text: info.title }),
    el("p", { text: info.summary }),
    sdkList(info.sdks, info.aiSdks)
  );

  const tracks = el(
    "div",
    { class: "panel" },
    el("h3", { class: "panel-title", text: "Tracks" }),
    info.launchable.length
      ? el("div", { class: "launch-grid" }, ...info.launchable.map((course) => trackTile(course, info)))
      : el("p", { class: "ws-note", text: "No track published yet." }),
    el(
      "p",
      { class: "ws-note track-access" },
      "No Instruqt access yet? ",
      externalLink(INSTRUQT_ACCESS_URL, {}, "Request it in Serval ↗")
    )
  );

  const assetCols = el(
    "div",
    { class: "asset-cols" },
    el(
      "div",
      { class: "panel" },
      el("h3", { class: "panel-title", text: "Presenting" }),
      el(
        "div",
        { class: "asset-list" },
        assetRow("Speaker notes", assetUrl("Speaker notes")),
        assetRow("Slide deck", assetUrl("Slide deck")),
        assetRow("Golden path recording", assetUrl("Golden path recording")),
        assetRow("External recording", assetUrl("External recording"))
      )
    ),
    el(
      "div",
      { class: "panel" },
      el("h3", { class: "panel-title", text: "Resources" }),
      el(
        "div",
        { class: "asset-list" },
        assetRow("Runbook", assetUrl("Runbook")),
        ...courseResources(members).map((resource) => assetRow(resource.label, resource.url))
      )
    )
  );

  const topics = el(
    "div",
    { class: "panel" },
    el("h3", { class: "panel-title", text: "Topics" }),
    tagRow(topicTags(info.tags)) || el("p", { class: "ws-note", text: "No topics tagged." })
  );

  return [head, el("div", { class: "drawer-body" }, title, tracks, assetCols, topics)];
}

function renderWorkshopsList() {
  const list = document.querySelector("[data-workshops-list]");
  const groups = filteredCourseGroups();
  const total = groupCourses(state.courses).length;
  const count = document.querySelector("[data-workshops-count]");
  count.hidden = !workshopsHaveFilters();
  count.textContent = `${groups.length} of ${plural(total, "workshop")}`;
  if (!groups.length) {
    list.replaceChildren(
      el("p", {
        class: "empty",
        text: state.courses.length
          ? "No workshops match these filters."
          : "No Instruqt workshops have been added yet.",
      })
    );
    return;
  }
  list.replaceChildren(...groups.map(renderWorkshopCard));
}

// ─── Namespace access (self-service) ──────────────────────────────────────
// Adds the signed-in user to the Temporal Cloud user group that can read every
// demo namespace. The server takes the email from the session, so this only
// ever grants the person clicking.

function setupNamespaceAccess() {
  const button = document.querySelector("[data-grant-access]");
  if (button) button.addEventListener("click", () => grantNamespaceAccess(button));
}

async function grantNamespaceAccess(button) {
  if (button.getAttribute("aria-busy") === "true") return;
  const label = button.querySelector("span") || button;
  const idleText = label.textContent;
  button.setAttribute("aria-busy", "true");
  label.textContent = "Granting Namespace Access…";
  try {
    const res = await fetch("/api/namespace-access", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    if (res.status === 401) {
      // Not signed in yet: sign in with Google, then land back here.
      window.location.assign(
        `/auth/login?return_to=${encodeURIComponent(window.location.href)}`
      );
      return;
    }
    let data = {};
    try {
      data = await res.json();
    } catch (err) {
      data = {};
    }
    if (!res.ok) throw new Error(data.message || data.error || `HTTP ${res.status}`);
    showNotice(namespaceAccessNotice(data));
  } catch (err) {
    showNotice({
      tone: "error",
      title: "Couldn't grant access",
      body: `The operator couldn't be reached: ${err.message}`,
    });
  } finally {
    button.removeAttribute("aria-busy");
    label.textContent = idleText;
  }
}

function namespaceAccessNotice(result) {
  const email = result.email || "your account";
  const group = result.group || "the namespace readers group";
  switch (result.status) {
    case "added":
      return {
        tone: "success",
        title: "You're in",
        body: `Added ${email} to ${group}, which can read every demo namespace. Temporal Cloud can take a minute to apply it.`,
        showNamespaces: true,
      };
    case "already-member":
      return {
        tone: "success",
        title: "You already have access",
        body: `${email} is already in ${group}, which can read every demo namespace.`,
        showNamespaces: true,
      };
    case "group-missing":
      return {
        tone: "warn",
        title: "Access isn't set up yet",
        body: result.message || `The ${group} group doesn't exist yet. A Temporal Cloud account admin needs to create it once.`,
      };
    case "user-not-found":
      return {
        tone: "warn",
        title: "No Temporal Cloud user for you yet",
        body: `We couldn't find a Temporal Cloud user with the email ${email}. Ask a Temporal Cloud admin to invite you to the account, then try again.`,
      };
    default:
      return {
        tone: "error",
        title: "Couldn't grant access",
        body: result.message || "The operator couldn't update the group. Try again in a minute.",
      };
  }
}

function showNotice({ tone, title, body, showNamespaces = false }) {
  document.querySelectorAll(".notice").forEach((node) => node.remove());
  const namespacesLink = document.querySelector("[data-namespaces-link]");
  const notice = el(
    "div",
    { class: `notice is-${tone}`, role: tone === "success" ? "status" : "alert" },
    el("strong", { text: title }),
    el(
      "button",
      {
        type: "button",
        class: "icon-btn",
        "aria-label": "Dismiss",
        onclick: () => notice.remove(),
      },
      icon("close")
    ),
    el("p", { text: body }),
    showNamespaces && namespacesLink
      ? externalLink(namespacesLink.href, {}, "See all namespaces ↗")
      : null
  );
  document.body.appendChild(notice);
  if (tone === "success") setTimeout(() => notice.remove(), 15000);
}

// ─── Render orchestration ─────────────────────────────────────────────────

function renderStats(selector, entries) {
  document
    .querySelector(selector)
    .replaceChildren(
      ...entries.map(([value, label]) =>
        el("div", {}, el("dt", { text: label }), el("dd", { text: String(value) }))
      )
    );
}

// Tab badges plus the page-header stat rows on both views.
function renderCounts() {
  const workshops = groupCourses(state.courses).length;
  document.querySelector('[data-tab-count="demos"]').textContent = String(state.demos.length);
  document.querySelector('[data-tab-count="workshops"]').textContent = String(workshops);
  renderStats("[data-demo-stats]", [
    [state.demos.length, "Demos"],
    [state.demos.filter((demo) => demo.canonical).length, "Canonical"],
    [state.demos.filter((demo) => demo.crashable).length, "Crashable"],
    [new Set(state.demos.flatMap((demo) => demo.sdks)).size, "SDKs"],
  ]);
  renderStats("[data-workshop-stats]", [
    [workshops, "Workshops"],
    [state.courses.length, "Tracks"],
    [new Set(state.courses.flatMap(courseSdks)).size, "SDKs"],
  ]);
}

function render() {
  const view = currentView();
  document.querySelectorAll("[data-view]").forEach((section) => {
    section.hidden = section.getAttribute("data-view") !== view;
  });
  document.querySelectorAll(".tabs [data-route]").forEach((link) => {
    if (link.getAttribute("data-route") === (view === "workshops" ? WORKSHOPS_ROUTE : "/")) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });
  // The header's access buttons follow the page: Temporal Cloud namespaces for
  // demos, Instruqt for workshops.
  document.querySelectorAll("[data-header-for]").forEach((node) => {
    const forWorkshops = node.getAttribute("data-header-for") === "workshops";
    node.hidden = forWorkshops !== (view === "workshops");
  });
  document.title = {
    workshops: "Workshops · Temporal Field Kit",
    onboard: "Add a Demo · Temporal Field Kit",
  }[view] || "Demos · Temporal Field Kit";
  renderCounts();
  renderZiggyLauncher();
  if (view === "onboard") {
    if (!onboard.loaded) loadOnboarding();
    renderOnboarding();
  } else if (view === "workshops") {
    renderWorkshopsFilters();
    renderWorkshopsList();
  } else {
    renderDemosFilters();
    renderDemosList();
  }
}

// ─── Onboarding ────────────────────────────────────────────────────────────
// /onboard drives the signed-in user's CatalogOnboardingWorkflow: pick a repo,
// edit the draft DemoProject (form or YAML), then hand off by opening a pull
// request on GitHub. The workflow validates every edit; this page edits a
// working copy and shows what the workflow says about it.

const ONBOARD_CLOSED = new Set(["HandedOff", "Abandoned", "Expired"]);

const onboard = {
  loaded: false,
  status: null, // last status from the workflow ({phase: "None"} when there is none)
  working: null, // the form's working copy of status.document
  yamlText: null, // unsaved YAML tab text
  tab: "form",
  busy: null, // label for the request in flight
  error: null, // last request error, shown above the steps
  signInUrl: null,
  changingRepo: false,
  askOpen: null, // pointer of the form section whose "Ask the agent" box is open
  agentMessage: "", // unsent text in the agent conversation's message box
};

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function getIn(obj, path) {
  return path.reduce((node, key) => (node == null ? undefined : node[key]), obj);
}

// Sets a nested value, creating parents. An empty value deletes the key and
// prunes parents it leaves empty, so clearing a field removes it from the YAML.
function setIn(obj, path, value) {
  const empty =
    value === undefined || value === "" || (Array.isArray(value) && value.length === 0);
  const parents = [obj];
  let node = obj;
  for (const key of path.slice(0, -1)) {
    if (node[key] == null || typeof node[key] !== "object") {
      if (empty) return;
      node[key] = {};
    }
    node = node[key];
    parents.push(node);
  }
  const last = path[path.length - 1];
  if (empty) delete node[last];
  else node[last] = value;
  for (let i = parents.length - 1; i > 0; i -= 1) {
    if (Object.keys(parents[i]).length > 0) break;
    delete parents[i - 1][path[i - 1]];
  }
}

// Renames an object key in place, keeping key order.
function renameKey(obj, from, to) {
  const entries = Object.entries(obj).map(([key, value]) => [key === from ? to : key, value]);
  Object.keys(obj).forEach((key) => delete obj[key]);
  entries.forEach(([key, value]) => (obj[key] = value));
}

function onboardDirty() {
  const status = onboard.status || {};
  if (onboard.tab === "yaml") return onboard.yamlText !== null && onboard.yamlText !== status.yaml;
  return JSON.stringify(onboard.working) !== JSON.stringify(status.document);
}

function acceptOnboardStatus(status) {
  onboard.status = status;
  onboard.working = clone(status.document) || null;
  onboard.yamlText = null;
  onboard.changingRepo = false;
}

async function loadOnboarding() {
  const preview = loadPreview();
  try {
    const res = await fetch("/api/onboarding", { credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (res.status === 401) {
      onboard.signInUrl = `/auth/login?return_to=${encodeURIComponent(window.location.href)}`;
    } else if (!res.ok) {
      onboard.error = data.message || data.error || `HTTP ${res.status}`;
    } else {
      acceptOnboardStatus(data);
    }
  } catch (err) {
    onboard.error = err.message;
  }
  await preview;
  onboard.loaded = true;
  renderOnboarding();
}

async function onboardCall(label, steps) {
  if (onboard.busy) return;
  onboard.busy = label;
  onboard.error = null;
  renderOnboarding();
  try {
    for (const [url, body] of steps) acceptOnboardStatus(await postJSON(url, body));
  } catch (err) {
    onboard.error = err.message;
  } finally {
    onboard.busy = null;
    renderOnboarding();
  }
}

function onboardOpen() {
  const phase = onboard.status && onboard.status.phase;
  return phase && phase !== "None" && !ONBOARD_CLOSED.has(phase);
}

function chooseRepo(repoUrl) {
  if (!repoUrl.trim()) return;
  if (onboard.status && onboard.status.repo && !window.confirm("Changing the repo replaces the draft. Continue?")) {
    return;
  }
  const steps = onboardOpen() ? [] : [["/api/onboarding", {}]];
  steps.push(["/api/onboarding/repo", { repo_url: repoUrl }]);
  onboardCall("Checking repository", steps);
}

function saveDraft() {
  const body = { base_revision: onboard.status.revision };
  if (onboard.tab === "yaml") body.yaml = onboard.yamlText ?? onboard.status.yaml;
  else body.document = onboard.working;
  onboardCall("Validating draft", [["/api/onboarding/draft", body]]);
}

function discardDraftChanges() {
  acceptOnboardStatus(onboard.status);
  renderOnboarding();
}

function switchOnboardTab(tab) {
  if (tab === onboard.tab) return;
  if (onboardDirty()) {
    toast("Validate or discard your changes first");
    return;
  }
  onboard.tab = tab;
  renderOnboarding();
}

// ─── Onboarding: the agent ─────────────────────────────────────────────────
// A conversation with the agent: the user asks it to draft the project, asks
// about one section, or answers its questions, and it remembers the earlier
// turns. Each reply takes minutes, so the page polls status and refreshes only
// the thread, leaving the form, the message box and unsaved edits alone.

const AGENT_SEVERITY_LABEL = { blocker: "Blocker", warning: "Warning", suggestion: "Suggestion" };
let onboardPollTimer = null;

function agentRunning(status) {
  return Boolean(status && status.agent && status.agent.phase === "Running");
}

function scheduleOnboardPoll() {
  clearTimeout(onboardPollTimer);
  if (agentRunning(onboard.status) && currentView() === "onboard") {
    onboardPollTimer = setTimeout(refreshAgentPanel, 3000);
  }
}

async function refreshAgentPanel() {
  const wasRunning = agentRunning(onboard.status);
  try {
    const res = await fetch("/api/onboarding", { credentials: "same-origin" });
    if (res.ok) {
      const data = await res.json();
      // The agent never edits the draft, so only its own state is new here.
      onboard.status = {
        ...onboard.status,
        agent: data.agent,
        agent_runs: data.agent_runs,
        conversation: data.conversation,
        agent_cost_usd: data.agent_cost_usd,
      };
    }
  } catch (err) {
    // Keep polling; a blip shouldn't end the run's display.
  }
  refreshAgentThread();
  if (wasRunning !== agentRunning(onboard.status)) {
    // The run ended: the message box can send again. Typing happens while the
    // agent works, so this is the only time the box is rebuilt.
    const composer = document.querySelector("[data-onboard-agent-composer]");
    if (composer) composer.replaceChildren(...agentComposerBody(onboard.status));
    const head = document.querySelector("[data-onboard-agent-head]");
    if (head) head.replaceChildren(...agentHeadBody(onboard.status));
  }
  scheduleOnboardPoll();
}

async function askAgent(body) {
  await onboardCall("Starting the agent", [["/api/onboarding/agent", body]]);
  if (body.pointer) {
    // Asked from a form section: show where the answer will appear.
    const panel = document.querySelector(".onboard-agent");
    if (panel) panel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function sendAgentMessage() {
  const message = onboard.agentMessage.trim();
  if (!message || agentRunning(onboard.status) || onboard.busy) return;
  if (onboardDirty()) {
    toast("Validate your changes first; the agent works from the saved draft");
    return;
  }
  onboard.agentMessage = "";
  askAgent({ mode: "chat", question: message });
}

function newAgentConversation() {
  if (!window.confirm("Start a new conversation? The agent forgets this one.")) return;
  onboardCall("Starting a new conversation", [["/api/onboarding/agent/new", {}]]);
}

function applyProposal(proposal) {
  if (onboardDirty() && !window.confirm("Replace your unsaved changes with the agent's draft?")) return;
  onboard.tab = "form";
  onboardCall("Applying the agent's draft", [
    ["/api/onboarding/draft", { base_revision: onboard.status.revision, document: proposal.document }],
  ]);
}

function downloadPatch(patch) {
  const url = URL.createObjectURL(new Blob([patch], { type: "text/x-diff" }));
  const link = el("a", { href: url, download: "onboarding.patch" });
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function copyPrompt(prompt) {
  navigator.clipboard.writeText(prompt).then(
    () => toast("Prompt copied"),
    () => toast("Couldn't copy the prompt")
  );
}

// What the latest reply proposes. In the thread the reply's text is already
// shown, so only a proposal without a conversation (an older run) repeats it.
function proposalView(proposal, { inThread = false } = {}) {
  const errors = proposal.errors || [];
  const changed =
    proposal.document && JSON.stringify(proposal.document) !== JSON.stringify(onboard.status.document);
  const parts = [];
  if (!inThread && proposal.question) {
    parts.push(el("p", { class: "onboard-hint", text: `You asked about ${proposal.pointer || "the draft"}: ${proposal.question}` }));
  }
  if (!inThread && proposal.summary) parts.push(el("p", { class: "onboard-agent-summary", text: proposal.summary }));
  if (changed) {
    parts.push(
      el(
        "div",
        { class: "onboard-actions" },
        el("button", {
          type: "button",
          class: "btn btn-primary btn-sm",
          "aria-disabled": String(Boolean(onboard.busy)),
          text: proposal.mode === "section" ? "Apply the suggested change" : "Apply the agent's draft",
          onclick: () => applyProposal(proposal),
        }),
        el("span", {
          class: "mono",
          text: errors.length ? `${errors.length} validator problem${errors.length === 1 ? "" : "s"} to fix after applying` : "Passes the validator",
        })
      )
    );
  } else if (proposal.mode === "section") {
    parts.push(el("p", { class: "onboard-hint", text: "No change suggested for the draft." }));
  }
  const findings = proposal.findings || [];
  if (findings.length) {
    parts.push(
      el(
        "div",
        { class: "onboard-findings" },
        el("strong", { text: "Catalog readiness" }),
        el(
          "ul",
          {},
          ...findings.map((finding) =>
            el(
              "li",
              { class: `is-${finding.severity}` },
              el("span", { class: "onboard-severity", text: AGENT_SEVERITY_LABEL[finding.severity] || finding.severity }),
              el("span", {}, el("strong", { text: finding.title }), ` ${finding.detail}`),
              finding.location ? el("code", { text: finding.location }) : null
            )
          )
        )
      )
    );
  }
  if (proposal.patch || proposal.prompt) {
    parts.push(
      el(
        "div",
        { class: "onboard-actions" },
        el("span", { class: "onboard-hint", text: "Suggested changes to the demo repo:" }),
        proposal.patch
          ? el("button", { type: "button", class: "btn btn-secondary btn-sm", text: "Download patch", onclick: () => downloadPatch(proposal.patch) })
          : null,
        proposal.prompt
          ? el("button", { type: "button", class: "btn btn-secondary btn-sm", text: "Copy Claude Code prompt", onclick: () => copyPrompt(proposal.prompt) })
          : null
      )
    );
  }
  const fields = proposal.fields || [];
  if (fields.length) {
    parts.push(
      el(
        "details",
        { class: "onboard-reasons" },
        el("summary", { text: `Why each field is set (${fields.length})` }),
        el(
          "ul",
          {},
          ...fields.map((item) =>
            el(
              "li",
              {},
              el("code", { text: item.pointer }),
              el("span", { text: item.reason }),
              (item.evidence || []).length ? el("span", { class: "mono", text: item.evidence.join(", ") }) : null
            )
          )
        )
      )
    );
  }
  parts.push(el("span", { class: "mono", text: `${proposal.num_turns} steps · $${Number(proposal.cost_usd || 0).toFixed(2)}` }));
  return parts;
}

function userMessageView(message) {
  return el(
    "li",
    { class: "onboard-msg is-user" },
    el("span", { class: "onboard-msg-who", text: "You" }),
    message.pointer ? el("code", { class: "onboard-msg-about", text: message.pointer }) : null,
    el("p", { text: message.text })
  );
}

function agentMessageView(message, status) {
  const agent = status.agent;
  const parts = [el("span", { class: "onboard-msg-who", text: "Agent" })];
  if (message.outcome === "Failed") {
    parts.push(onboardAlert("error", "The agent didn't finish.", message.text));
  } else if (message.outcome === "Cancelled") {
    parts.push(el("p", { class: "onboard-hint", text: "Stopped before answering." }));
  } else {
    parts.push(el("p", { class: "onboard-agent-summary", text: message.text }));
  }
  const questions = message.questions || [];
  if (questions.length) {
    parts.push(
      el(
        "div",
        { class: "onboard-msg-questions" },
        el("strong", { text: questions.length === 1 ? "The agent asks" : "The agent asks you" }),
        el("ul", {}, ...questions.map((question) => el("li", { text: question })))
      )
    );
  }
  // Only the latest reply's proposal is kept; earlier ones were applied or not.
  const latest = agent && agent.phase === "Done" && agent.proposal && message.run === status.agent_runs;
  if (latest) parts.push(...proposalView(agent.proposal, { inThread: true }));
  return el("li", { class: "onboard-msg is-agent" }, ...parts);
}

function agentRunningView(agent) {
  if (agent.stopping) return el("p", { class: "onboard-busy", role: "status", text: "Stopping the agent…" });
  return el(
    "div",
    { class: "onboard-msg is-agent is-working" },
    el("p", { class: "onboard-busy", role: "status", text: `${agent.mode === "draft" ? "Drafting" : "Working"}: ${agent.progress || "starting"}…` }),
    el(
      "div",
      { class: "onboard-actions" },
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        text: "Stop the agent",
        onclick: () => onboardCall("Stopping the agent", [["/api/onboarding/agent/cancel", {}]]),
      })
    )
  );
}

function agentPanelBody(status) {
  const agent = status.agent;
  const conversation = status.conversation || [];
  const parts = [];
  if (!conversation.length) {
    parts.push(
      el("p", {
        class: "onboard-hint",
        text: "The agent reads the repo, writes the whole project config with a reason for each field, and points out what the repo needs to change to run in the catalog. Ask it to draft the project, or ask it anything about the repo; it remembers the conversation. Each reply takes a few minutes, and you can keep editing meanwhile.",
      })
    );
    // An agent run from before conversations existed.
    if (agent && agent.phase === "Done" && agent.proposal) parts.push(...proposalView(agent.proposal));
  } else {
    parts.push(
      el(
        "ol",
        { class: "onboard-thread", "aria-label": "Conversation with the agent" },
        ...conversation.map((message) =>
          message.role === "user" ? userMessageView(message) : agentMessageView(message, status)
        )
      )
    );
  }
  if (agentRunning(status)) parts.push(agentRunningView(agent));
  return parts;
}

// Rebuilds the thread in place. A reader scrolled back through it stays where
// they are; one at the newest message follows new ones.
function refreshAgentThread() {
  const panel = document.querySelector("[data-onboard-agent]");
  if (!panel) return;
  const old = panel.querySelector(".onboard-thread");
  const atEnd = !old || old.scrollHeight - old.scrollTop - old.clientHeight < 24;
  const top = old ? old.scrollTop : 0;
  panel.replaceChildren(...agentPanelBody(onboard.status));
  const thread = panel.querySelector(".onboard-thread");
  if (thread) thread.scrollTop = atEnd ? thread.scrollHeight : top;
}

function scrollAgentThreadToEnd() {
  const thread = document.querySelector("[data-onboard-agent] .onboard-thread");
  if (thread) thread.scrollTop = thread.scrollHeight;
}

function agentHeadBody(status) {
  const conversation = status.conversation || [];
  const spent = Number(status.agent_cost_usd || 0);
  return [
    el("p", { class: "eyebrow", text: "Agent" }),
    spent ? el("span", { class: "mono", title: "What the agent has spent on this onboarding", text: `$${spent.toFixed(2)} spent` }) : null,
    conversation.length && !agentRunning(status)
      ? el("button", {
          type: "button",
          class: "btn btn-ghost btn-sm",
          "aria-disabled": String(Boolean(onboard.busy)),
          text: "New conversation",
          onclick: newAgentConversation,
        })
      : null,
  ];
}

function agentComposerBody(status) {
  const running = agentRunning(status);
  const blocked = running || Boolean(onboard.busy);
  const conversation = status.conversation || [];
  const box = el("textarea", {
    rows: 2,
    maxlength: 2000,
    placeholder: conversation.length ? "Reply to the agent, or ask it something else" : "Ask the agent about the repo",
    "aria-label": "Message the agent",
    text: onboard.agentMessage,
    oninput: (event) => {
      onboard.agentMessage = event.target.value;
    },
    onkeydown: (event) => {
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        sendAgentMessage();
      }
    },
  });
  const dirty = onboardDirty();
  return [
    box,
    el(
      "div",
      { class: "onboard-actions" },
      el("button", {
        type: "button",
        class: "btn btn-secondary btn-sm",
        "aria-disabled": String(blocked),
        title: running ? "Wait for the agent's reply, or stop it." : null,
        text: "Send",
        onclick: sendAgentMessage,
      }),
      el("button", {
        type: "button",
        class: conversation.length ? "btn btn-quiet btn-sm" : "btn btn-pink btn-sm",
        "aria-disabled": String(blocked || dirty),
        title: dirty ? "Validate your changes first; the agent works from the saved draft." : null,
        text: conversation.length ? "Draft it again" : "Draft with the agent",
        onclick: () => {
          if (onboardDirty()) {
            toast("Validate your changes first; the agent works from the saved draft");
            return;
          }
          askAgent({ mode: "draft" });
        },
      }),
      el("span", { class: "onboard-hint", text: "⌘/Ctrl + Enter to send" })
    ),
  ];
}

function agentPanel(status) {
  return el(
    "div",
    { class: "onboard-agent" },
    el("div", { class: "onboard-agent-head", "data-onboard-agent-head": "" }, ...agentHeadBody(status)),
    el("div", { "data-onboard-agent": "" }, ...agentPanelBody(status)),
    el("form", { class: "onboard-agent-composer", "data-onboard-agent-composer": "", onsubmit: (event) => event.preventDefault() }, ...agentComposerBody(status))
  );
}

// "Ask the agent" under a form section: one question about that part.
function askControl(pointer) {
  if (onboard.askOpen !== pointer) {
    return el("button", {
      type: "button",
      class: "btn btn-ghost btn-sm onboard-ask",
      "aria-disabled": String(agentRunning(onboard.status) || Boolean(onboard.busy)),
      text: "Ask the agent about this",
      onclick: () => {
        onboard.askOpen = pointer;
        renderOnboarding();
      },
    });
  }
  const question = el("textarea", {
    rows: 2,
    maxlength: 2000,
    placeholder: "e.g. Which of these processes are Temporal workers?",
    "aria-label": "Question for the agent",
  });
  return el(
    "form",
    {
      class: "onboard-ask-form",
      onsubmit: (event) => {
        event.preventDefault();
        if (onboardDirty()) {
          toast("Validate your changes first");
          return;
        }
        onboard.askOpen = null;
        askAgent({ mode: "section", pointer, question: question.value });
      },
    },
    question,
    el(
      "div",
      { class: "onboard-actions" },
      el("button", { type: "submit", class: "btn btn-secondary btn-sm", text: "Ask" }),
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        text: "Cancel",
        onclick: () => {
          onboard.askOpen = null;
          renderOnboarding();
        },
      })
    )
  );
}

// ─── Onboarding: secrets ───────────────────────────────────────────────────
// Project secret values go straight from this form to AWS Secrets Manager
// (POST /api/onboarding/secrets); the workflow and the draft only ever hold
// names. The draft declares each secret under spec.secrets and lists it on the
// components that need it.

const onboardSecrets = {
  project: undefined, // the project the list below was loaded for
  loading: false,
  stored: [], // project secrets in Secrets Manager: {name, properties, owner, onboarding}
  // Secrets this page stored, by project. ListSecrets is eventually consistent,
  // so a list right after a store can miss the new secret; these fill the gap.
  written: {},
  shared: [], // platform shared secrets: {name, properties, description}
  error: null,
  editing: null, // {name, rows: [{key, value}]} while the store form is open
};

function draftProjectName(status) {
  const name = getIn(status && status.document, ["metadata", "name"]);
  return typeof name === "string" ? name : null;
}

async function loadSecrets() {
  const project = draftProjectName(onboard.status);
  onboardSecrets.loading = true;
  onboardSecrets.project = project;
  try {
    const res = await fetch("/api/onboarding/secrets", { credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.message || data.error || `HTTP ${res.status}`);
    const listed = data.secrets || [];
    const written = Object.values(onboardSecrets.written[project] || {}).filter(
      (entry) => !listed.some((item) => item.name === entry.name)
    );
    onboardSecrets.stored = [...listed, ...written].sort((a, b) => a.name.localeCompare(b.name));
    onboardSecrets.shared = data.shared || [];
    onboardSecrets.error = null;
  } catch (err) {
    onboardSecrets.error = err.message;
  } finally {
    onboardSecrets.loading = false;
    const section = document.querySelector("[data-onboard-secrets]");
    if (section && onboard.working) section.replaceChildren(...secretsBody(onboard.working));
  }
}

function secretsNeedLoading() {
  return (
    !onboardSecrets.loading &&
    onboard.status &&
    onboard.status.repo &&
    onboardSecrets.project !== draftProjectName(onboard.status)
  );
}

function rerenderSecrets() {
  const section = document.querySelector("[data-onboard-secrets]");
  if (section) section.replaceChildren(...secretsBody(onboard.working));
  renderOnboardingActions();
}

function componentSecretNames(component) {
  return Array.isArray(component.secrets) ? component.secrets : [];
}

function setComponentSecret(component, name, on) {
  const names = componentSecretNames(component).filter((item) => item !== name);
  if (on) names.push(name);
  setIn(component, ["secrets"], names.length ? names : undefined);
}

function removeSecret(doc, name) {
  setIn(doc, ["spec", "secrets", name], undefined);
  Object.values(getIn(doc, ["spec", "components"]) || {}).forEach((component) =>
    setComponentSecret(component, name, false)
  );
}

async function storeSecret(doc) {
  const editing = onboardSecrets.editing;
  const values = {};
  for (const row of editing.rows) {
    const key = row.key.trim();
    if (key) values[key] = row.value;
  }
  try {
    const result = await postJSON("/api/onboarding/secrets", { name: editing.name.trim(), values });
    const existing = getIn(doc, ["spec", "secrets", result.name]) || {};
    setIn(doc, ["spec", "secrets", result.name], { ...existing, requiredProperties: result.properties });
    delete getIn(doc, ["spec", "secrets", result.name]).shared;
    onboardSecrets.editing = null; // drops the typed values
    toast(result.created ? "Secret stored" : "Secret updated");
    const project = result.project || draftProjectName(onboard.status);
    onboardSecrets.written[project] = {
      ...onboardSecrets.written[project],
      [result.name]: { name: result.name, properties: result.properties, onboarding: true },
    };
    onboardSecrets.project = undefined;
    await loadSecrets();
  } catch (err) {
    onboardSecrets.error = err.message;
  }
  rerenderSecrets();
}

function secretEditor(doc) {
  const editing = onboardSecrets.editing;
  const savedName = draftProjectName(onboard.status);
  const renamed = savedName && getIn(doc, ["metadata", "name"]) !== savedName;
  return el(
    "form",
    {
      class: "onboard-secret-editor",
      autocomplete: "off",
      onsubmit: (event) => {
        event.preventDefault();
        storeSecret(doc);
      },
    },
    field(
      "Secret name",
      null,
      textControl(editing.name, (value) => (editing.name = value), { placeholder: "app-credentials" }),
      `Stored at tmprl-dem-cld/${savedName || "<project>"}/<name>. Values go straight to AWS Secrets Manager; nobody can read them back from here.`
    ),
    renamed
      ? onboardAlert("warn", "Validate your rename first.", `Secrets are stored under the saved project name, ${savedName}.`)
      : null,
    ...editing.rows.map((row, index) =>
      el(
        "div",
        { class: "onboard-secret-row" },
        textControl(row.key, (value) => (row.key = value), { placeholder: "API_KEY", "aria-label": "Property name" }),
        Object.assign(
          el("input", {
            type: "password",
            autocomplete: "new-password",
            placeholder: "value",
            "aria-label": "Property value",
            oninput: (event) => (row.value = event.target.value),
          }),
          // A property, not an attribute, so the value never lands in the HTML.
          { value: row.value }
        ),
        editing.rows.length > 1
          ? el("button", {
              type: "button",
              class: "btn btn-quiet btn-sm",
              text: "Remove",
              onclick: () => {
                editing.rows.splice(index, 1);
                rerenderSecrets();
              },
            })
          : null
      )
    ),
    el(
      "div",
      { class: "onboard-actions" },
      el("button", {
        type: "button",
        class: "btn btn-ghost btn-sm",
        text: "+ Property",
        onclick: () => {
          editing.rows.push({ key: "", value: "" });
          rerenderSecrets();
        },
      }),
      el("button", { type: "submit", class: "btn btn-primary btn-sm", "aria-disabled": String(Boolean(renamed)), text: "Store in Secrets Manager" }),
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        text: "Cancel",
        onclick: () => {
          onboardSecrets.editing = null;
          rerenderSecrets();
        },
      })
    )
  );
}

function declaredSecretRow(doc, name, definition) {
  const components = getIn(doc, ["spec", "components"]) || {};
  const shared = typeof definition.shared === "string" ? definition.shared : null;
  const stored = onboardSecrets.stored.find((item) => item.name === name);
  let state;
  if (shared) {
    const known = onboardSecrets.shared.find((item) => item.name === shared);
    state = `Shared: ${shared}${known && known.properties.length ? ` (${known.properties.join(", ")})` : ""}`;
  } else if (stored) {
    state = `Stored${stored.properties.length ? ` (${stored.properties.join(", ")})` : ""}`;
  } else {
    state = "Not stored yet";
  }
  return el(
    "div",
    { class: "onboard-card", dataset: { pointer: `/spec/secrets/${name}` } },
    el(
      "div",
      { class: "onboard-secret-head" },
      el("strong", { text: name }),
      el("span", { class: `mono ${!shared && !stored ? "is-missing" : ""}`, text: state })
    ),
    Object.keys(components).length
      ? el(
          "div",
          { class: "onboard-checks" },
          ...Object.entries(components).map(([componentName, component]) =>
            checkControl(
              `Give it to ${componentName}`,
              `/spec/components/${componentName}/secrets`,
              componentSecretNames(component).includes(name),
              (on) => setComponentSecret(component, name, on)
            )
          )
        )
      : null,
    el(
      "div",
      { class: "onboard-actions" },
      !shared
        ? el("button", {
            type: "button",
            class: "btn btn-ghost btn-sm",
            text: stored ? "Replace the value" : "Store a value",
            onclick: () => {
              const keys = (definition.requiredProperties || []).length
                ? definition.requiredProperties
                : (stored && stored.properties) || [""];
              onboardSecrets.editing = { name, rows: keys.map((key) => ({ key, value: "" })) };
              rerenderSecrets();
            },
          })
        : null,
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        text: "Remove from the draft",
        onclick: () => {
          removeSecret(doc, name);
          rerenderSecrets();
        },
      })
    )
  );
}

function sharedPicker(doc) {
  const declared = new Set(
    Object.values(getIn(doc, ["spec", "secrets"]) || {}).map((definition) => definition.shared)
  );
  const options = onboardSecrets.shared.filter((item) => !declared.has(item.name));
  if (!options.length) return null;
  const select = el(
    "select",
    { "aria-label": "Shared secret" },
    ...options.map((item) =>
      el("option", { value: item.name, text: `${item.name}${item.properties.length ? ` (${item.properties.join(", ")})` : ""}` })
    )
  );
  return el(
    "div",
    { class: "onboard-inline" },
    select,
    el("button", {
      type: "button",
      class: "btn btn-ghost btn-sm",
      text: "Use this shared secret",
      onclick: () => {
        const item = onboardSecrets.shared.find((candidate) => candidate.name === select.value);
        const name = item.name.replace(/[^a-z0-9-]+/g, "-");
        setIn(doc, ["spec", "secrets", name], {
          shared: item.name,
          ...(item.properties.length ? { requiredProperties: item.properties } : {}),
        });
        rerenderSecrets();
      },
    })
  );
}

function secretsBody(doc) {
  const declared = getIn(doc, ["spec", "secrets"]) || {};
  const parts = [
    el("legend", { text: "Secrets" }),
    el("p", {
      class: "onboard-hint",
      text: "API keys and other credentials the demo reads from environment variables. Each property of a secret becomes an environment variable on the components you give it to.",
    }),
  ];
  if (onboardSecrets.error) parts.push(onboardAlert("error", "Secrets", onboardSecrets.error));
  parts.push(...Object.entries(declared).map(([name, definition]) => declaredSecretRow(doc, name, definition || {})));
  if (onboardSecrets.editing) {
    parts.push(secretEditor(doc));
  } else {
    parts.push(
      el(
        "div",
        { class: "onboard-actions" },
        el("button", {
          type: "button",
          class: "btn btn-ghost btn-sm",
          text: "+ Add a project secret",
          onclick: () => {
            onboardSecrets.editing = { name: "", rows: [{ key: "", value: "" }] };
            rerenderSecrets();
          },
        })
      ),
      sharedPicker(doc)
    );
  }
  return parts;
}

function secretsFields(doc) {
  if (secretsNeedLoading()) loadSecrets();
  return el("fieldset", { class: "onboard-fields onboard-wide", "data-onboard-secrets": "" }, ...secretsBody(doc));
}

// ─── Onboarding: rendering ─────────────────────────────────────────────────

function onboardStep(number, title, ...children) {
  return el(
    "section",
    { class: "onboard-step" },
    el(
      "header",
      { class: "onboard-step-head" },
      el("p", { class: "eyebrow", text: `0${number}` }),
      el("h2", { text: title })
    ),
    ...children
  );
}

function onboardAlert(tone, title, body) {
  return el(
    "div",
    { class: `onboard-alert is-${tone}`, role: tone === "error" ? "alert" : "status" },
    el("strong", { text: title }),
    body ? el("p", { text: body }) : null
  );
}

function field(label, pointer, control, hint) {
  return el(
    "label",
    { class: "onboard-field", dataset: pointer ? { pointer } : null },
    el("span", { class: "onboard-label", text: label }),
    control,
    hint ? el("span", { class: "onboard-hint", text: hint }) : null
  );
}

function textControl(value, onInput, attrs = {}) {
  return el("input", {
    type: "text",
    value: value ?? "",
    spellcheck: "false",
    oninput: (event) => onInput(event.target.value),
    ...attrs,
  });
}

function checkControl(label, pointer, checked, onChange, { disabled = false, hint } = {}) {
  return el(
    "label",
    { class: "onboard-check", dataset: { pointer } },
    el("input", {
      type: "checkbox",
      checked: Boolean(checked),
      disabled,
      onchange: (event) => onChange(event.target.checked),
    }),
    el("span", { text: label }),
    hint ? el("span", { class: "onboard-hint", text: hint }) : null
  );
}

function listValue(value) {
  return Array.isArray(value) ? value.join(", ") : "";
}

function parseList(text) {
  return text
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function catalogFields(doc) {
  const spec = ["spec", "registry"];
  const set = (path) => (value) => setIn(doc, path, value);
  return el(
    "fieldset",
    { class: "onboard-fields" },
    el("legend", { text: "Catalog entry" }),
    askControl("/spec/registry"),
    field(
      "Project name",
      "/metadata/name",
      textControl(getIn(doc, ["metadata", "name"]), set(["metadata", "name"])),
      "Lowercase letters, digits and dashes; ideally 25 characters or fewer. Everything else is named after it."
    ),
    field("Title", "/spec/registry/title", textControl(getIn(doc, [...spec, "title"]), set([...spec, "title"]))),
    field(
      "Summary",
      "/spec/registry/summary",
      el("textarea", {
        rows: 3,
        oninput: (event) => setIn(doc, [...spec, "summary"], event.target.value),
        text: getIn(doc, [...spec, "summary"]) ?? "",
      })
    ),
    field(
      "Tags",
      "/spec/registry/tags",
      textControl(listValue(getIn(doc, [...spec, "tags"])), (text) => setIn(doc, [...spec, "tags"], parseList(text))),
      "Comma separated, e.g. Workflows, Signals."
    ),
    field(
      "Temporal SDKs",
      "/spec/registry/temporalSdks",
      textControl(listValue(getIn(doc, [...spec, "temporalSdks"])), (text) =>
        setIn(doc, [...spec, "temporalSdks"], parseList(text))
      ),
      "Comma separated, e.g. Go, Python."
    ),
    field(
      "Runbook URL",
      "/spec/registry/links/runbook",
      textControl(getIn(doc, [...spec, "links", "runbook", "url"]), set([...spec, "links", "runbook", "url"]), {
        type: "url",
        placeholder: "https://github.com/…#demo-script",
      })
    ),
    field(
      "Slide deck URL",
      "/spec/registry/links/slideDeck",
      textControl(getIn(doc, [...spec, "links", "slideDeck", "url"]), set([...spec, "links", "slideDeck", "url"]), {
        type: "url",
        placeholder: "https://docs.google.com/presentation/…",
      })
    )
  );
}

function buildRows(doc) {
  const builds = getIn(doc, ["spec", "builds"]) || {};
  const rows = Object.entries(builds).map(([name, build]) => {
    const pointer = `/spec/builds/${name}`;
    return el(
      "div",
      { class: "onboard-row", dataset: { pointer } },
      field(
        "Build",
        null, // the row carries the build's pointer
        textControl(name, () => {}, {
          onchange: (event) => {
            const next = event.target.value.trim();
            if (!next || next === name || builds[next]) return renderOnboarding();
            renameKey(builds, name, next);
            Object.values(getIn(doc, ["spec", "components"]) || {}).forEach((component) => {
              if (component.build === name) component.build = next;
            });
            renderOnboarding();
          },
        })
      ),
      field("Context", `${pointer}/context`, textControl(build.context, (value) => setIn(build, ["context"], value))),
      field(
        "Dockerfile",
        `${pointer}/dockerfile`,
        textControl(build.dockerfile, (value) => setIn(build, ["dockerfile"], value), { placeholder: "Dockerfile" })
      ),
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        text: "Remove",
        onclick: () => {
          delete builds[name];
          renderOnboarding();
        },
      })
    );
  });
  return el(
    "fieldset",
    { class: "onboard-fields onboard-wide" },
    el("legend", { text: "Builds" }),
    askControl("/spec/builds"),
    el("p", { class: "onboard-hint", text: "One container image per entry, built by CodeBuild from the repo." }),
    ...rows,
    el("button", {
      type: "button",
      class: "btn btn-ghost btn-sm",
      text: "+ Add build",
      onclick: () => {
        const spec = doc.spec || (doc.spec = {});
        spec.builds = spec.builds || {};
        let n = Object.keys(spec.builds).length + 1;
        while (spec.builds[`build-${n}`]) n += 1;
        spec.builds[`build-${n}`] = { context: "." };
        renderOnboarding();
      },
    })
  );
}

function componentCards(doc) {
  const components = getIn(doc, ["spec", "components"]) || {};
  const buildNames = Object.keys(getIn(doc, ["spec", "builds"]) || {});
  const cards = Object.entries(components).map(([name, component]) => {
    const pointer = `/spec/components/${name}`;
    const ports = Array.isArray(component.ports) ? component.ports : [];
    const port = ports[0] || {};
    const workerIsObject = component.worker && typeof component.worker === "object";
    const setPort = (key, value) => {
      if (value === undefined && Object.keys(port).every((k) => k === key)) {
        delete component.ports;
        return;
      }
      if (!ports.length) component.ports = [port];
      setIn(port, [key], value);
    };
    return el(
      "div",
      { class: "onboard-card", dataset: { pointer } },
      el(
        "div",
        { class: "onboard-card-fields" },
        field(
          "Component",
          null, // the card carries the component's pointer
          textControl(name, () => {}, {
            onchange: (event) => {
              const next = event.target.value.trim();
              if (!next || next === name || components[next]) return renderOnboarding();
              renameKey(components, name, next);
              renderOnboarding();
            },
          })
        ),
        field(
          "Build",
          `${pointer}/build`,
          el(
            "select",
            { onchange: (event) => setIn(component, ["build"], event.target.value) },
            ...[...new Set([...buildNames, component.build].filter(Boolean))].map((build) =>
              el("option", { value: build, selected: build === component.build, text: build })
            )
          )
        ),
        field(
          "Container port",
          `${pointer}/ports/0/containerPort`,
          textControl(port.containerPort, (value) => setPort("containerPort", value === "" ? undefined : Number(value)), {
            type: "number",
            min: 1,
            max: 65535,
            placeholder: "none",
          }),
          ports.length > 1 ? "Only the first port is shown; edit the rest in YAML." : null
        )
      ),
      el(
        "div",
        { class: "onboard-checks" },
        checkControl("Public (serves the demo's ingress on port 80)", `${pointer}/ports/0/servicePort`, port.servicePort === 80, (on) =>
          setPort("servicePort", on ? 80 : undefined)
        ),
        checkControl(
          "Temporal worker",
          `${pointer}/worker`,
          component.worker,
          (on) => {
            setIn(component, ["worker"], on ? true : undefined);
            if (!on) delete component.autoscaling;
            renderOnboarding();
          },
          { disabled: workerIsObject, hint: workerIsObject ? "Worker Versioning is configured in YAML." : null }
        ),
        checkControl("Starts or queries workflows (not a worker)", `${pointer}/temporalAccess`, component.temporalAccess, (on) =>
          setIn(component, ["temporalAccess"], on ? true : undefined)
        ),
        checkControl("Needs the local Temporal proxy", `${pointer}/temporalProxy`, getIn(component, ["temporalProxy", "enabled"]), (on) =>
          setIn(component, ["temporalProxy"], on ? { enabled: true } : undefined)
        )
      ),
      component.worker ? autoscalingFields(component, pointer) : null,
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        text: "Remove component",
        onclick: () => {
          delete components[name];
          renderOnboarding();
        },
      })
    );
  });
  return el(
    "fieldset",
    { class: "onboard-fields onboard-wide" },
    el("legend", { text: "Components" }),
    askControl("/spec/components"),
    el("p", { class: "onboard-hint", text: "One long-running process each. Exactly one should be public." }),
    ...cards,
    el("button", {
      type: "button",
      class: "btn btn-ghost btn-sm",
      text: "+ Add component",
      onclick: () => {
        const spec = doc.spec || (doc.spec = {});
        spec.components = spec.components || {};
        let n = Object.keys(spec.components).length + 1;
        while (spec.components[`component-${n}`]) n += 1;
        spec.components[`component-${n}`] = { build: buildNames[0] || "" };
        renderOnboarding();
      },
    })
  );
}

// Worker autoscaling (KEDA, on task-queue backlog). The common knobs live
// here; queueTypes, targetQueueSize and cooldownPeriod stay YAML-only.
function autoscalingFields(component, pointer) {
  const base = `${pointer}/autoscaling`;
  const fixed = component.replicas !== undefined;
  const disabled = getIn(component, ["autoscaling", "enabled"]) === false;
  const setNumber = (key) => (value) =>
    setIn(component, ["autoscaling", key], value === "" ? undefined : Number(value));
  if (fixed) {
    return el("p", {
      class: "onboard-hint",
      dataset: { pointer: `${pointer}/replicas` },
      text: `Fixed at ${component.replicas} replica(s) in YAML; remove replicas to autoscale.`,
    });
  }
  return el(
    "div",
    { class: "onboard-card-fields", dataset: { pointer: base } },
    checkControl(
      "Autoscale on task-queue backlog",
      `${base}/enabled`,
      !disabled,
      (on) => {
        setIn(component, ["autoscaling"], on ? undefined : { enabled: false });
        renderOnboarding();
      },
      { hint: "Off keeps a single fixed replica." }
    ),
    disabled
      ? null
      : field(
          "Task queue",
          `${base}/taskQueue`,
          textControl(
            getIn(component, ["autoscaling", "taskQueue"]),
            (value) => setIn(component, ["autoscaling", "taskQueue"], value.trim() || undefined),
            { placeholder: "from TEMPORAL_TASK_QUEUE" }
          ),
          "The queue this worker polls. Defaults to a literal TEMPORAL_TASK_QUEUE env or the crashable base task queue; with none, the worker stays at 1 replica."
        ),
    disabled
      ? null
      : field(
          "Min replicas",
          `${base}/minReplicas`,
          textControl(getIn(component, ["autoscaling", "minReplicas"]), setNumber("minReplicas"), {
            type: "number",
            min: 0,
            placeholder: "1",
          }),
          "0 scales to zero when idle; the first task then waits for a pod to start."
        ),
    disabled
      ? null
      : field(
          "Max replicas",
          `${base}/maxReplicas`,
          textControl(getIn(component, ["autoscaling", "maxReplicas"]), setNumber("maxReplicas"), {
            type: "number",
            min: 1,
            max: 20,
            placeholder: "10",
          }),
          "Queue types, target backlog and cooldown are YAML-only."
        )
  );
}

function runtimeFields(doc) {
  const crashable = ["spec", "registry", "crashable"];
  return el(
    "fieldset",
    { class: "onboard-fields" },
    el("legend", { text: "Ingress and crashable environments" }),
    askControl("/spec/ingress"),
    field(
      "Subdomain",
      "/spec/ingress/subdomain",
      textControl(getIn(doc, ["spec", "ingress", "subdomain"]), (value) => setIn(doc, ["spec", "ingress", "subdomain"], value)),
      "The demo is served at <subdomain>.tmprl-demo.cloud."
    ),
    field(
      "Health check path",
      "/spec/smokeCheck",
      textControl(getIn(doc, ["spec", "smokeCheck", "path"]), (value) => setIn(doc, ["spec", "smokeCheck", "path"], value), {
        placeholder: "/healthz",
      }),
      "A cheap GET that returns 200; rollouts wait for it."
    ),
    checkControl(
      "Require Temporal sign-in to open the demo",
      "/spec/ingress/temporalAuthRequired",
      getIn(doc, ["spec", "ingress", "temporalAuthRequired"]) !== false,
      (on) => setIn(doc, ["spec", "ingress", "temporalAuthRequired"], on ? undefined : false)
    ),
    checkControl(
      "Crashable: users can launch a private copy and kill its workers",
      "/spec/registry/crashable",
      getIn(doc, [...crashable, "enabled"]),
      (on) => {
        setIn(doc, crashable, on ? { ...getIn(doc, crashable), enabled: true } : undefined);
        renderOnboarding();
      }
    ),
    getIn(doc, [...crashable, "enabled"])
      ? el(
          "div",
          { class: "onboard-subfields" },
          field(
            "Task queue env var",
            "/spec/registry/crashable/taskQueueEnv",
            textControl(getIn(doc, [...crashable, "taskQueueEnv"]), (value) => setIn(doc, [...crashable, "taskQueueEnv"], value), {
              placeholder: "TASK_QUEUE",
            }),
            "Every process reads its task queue from this variable."
          ),
          field(
            "Base task queue",
            "/spec/registry/crashable/baseTaskQueue",
            textControl(getIn(doc, [...crashable, "baseTaskQueue"]), (value) => setIn(doc, [...crashable, "baseTaskQueue"], value))
          )
        )
      : null
  );
}

function errorList(errors) {
  if (!errors.length) return null;
  return el(
    "div",
    { class: "onboard-errors", role: "alert" },
    el("strong", { text: errors.length === 1 ? "1 problem" : `${errors.length} problems` }),
    el(
      "ul",
      {},
      ...errors.map((error) =>
        el(
          "li",
          {},
          error.pointer ? el("code", { text: error.pointer }) : null,
          el("span", { text: error.message })
        )
      )
    )
  );
}

// Marks every field an error points at, or inside of.
function markErrorFields(root, errors) {
  root.querySelectorAll("[data-pointer]").forEach((node) => {
    const pointer = node.dataset.pointer;
    const hit = errors.some(
      (error) => error.pointer && (error.pointer === pointer || error.pointer.startsWith(`${pointer}/`))
    );
    node.classList.toggle("has-error", hit);
  });
}

// ─── Onboarding: test deploy ───────────────────────────────────────────────
// A valid draft can run as a hidden preview (pv-<name>) for 48 hours. The
// preview is its own per-user workflow that outlives the onboarding, so the
// page reads and ends it separately, and keeps showing it after hand-off.

const PREVIEW_LIVE = new Set(["Deploying", "Ready", "Failed"]);
const PREVIEW_POLLING = new Set(["Deploying", "Ending"]);
// The operator's rollout stages, in order (ROLLOUT_STAGES in the operator).
const PREVIEW_STAGES = [
  ["resources", "Temporal Cloud and image repositories"],
  ["build", "Build images"],
  ["deploy", "Deploy"],
  ["rollout", "Roll out workers and check health"],
];
const onboardPreview = { status: null };
let previewPollTimer = null;

function previewLive(preview) {
  return Boolean(preview && PREVIEW_LIVE.has(preview.phase));
}

async function loadPreview() {
  try {
    const res = await fetch("/api/onboarding/preview", { credentials: "same-origin" });
    if (res.ok) onboardPreview.status = await res.json();
  } catch (err) {
    // Leave the last known state; the next poll or reload tries again.
  }
}

function schedulePreviewPoll() {
  clearTimeout(previewPollTimer);
  const preview = onboardPreview.status;
  if (preview && PREVIEW_POLLING.has(preview.phase) && currentView() === "onboard") {
    previewPollTimer = setTimeout(refreshPreviewPanel, 5000);
  }
}

async function refreshPreviewPanel() {
  await loadPreview();
  const panel = document.querySelector("[data-onboard-preview]");
  if (panel) panel.replaceChildren(...previewBody(onboard.status || {}));
  schedulePreviewPoll();
}

async function previewCall(label, url) {
  if (onboard.busy) return;
  onboard.busy = label;
  onboard.error = null;
  renderOnboarding();
  try {
    onboardPreview.status = await postJSON(url, {});
  } catch (err) {
    onboard.error = err.message;
  } finally {
    onboard.busy = null;
    renderOnboarding();
  }
}

function deployPreview() {
  previewCall("Starting the test deploy", "/api/onboarding/preview");
}

function endPreview() {
  if (window.confirm("End the test deploy and delete everything it created?")) {
    previewCall("Ending the test deploy", "/api/onboarding/preview/end");
  }
}

// One row per stage: done, the current one with its live step, or still to come.
function previewProgress(preview) {
  const current = Math.max(0, PREVIEW_STAGES.findIndex(([stage]) => stage === preview.stage));
  return el(
    "ol",
    { class: "onboard-progress", "aria-label": "Test deploy progress" },
    ...PREVIEW_STAGES.map(([, label], index) => {
      const state = index < current ? "done" : index === current ? "current" : "todo";
      return el(
        "li",
        { class: `is-${state}`, "aria-current": state === "current" ? "step" : null },
        el("span", { class: "onboard-progress-mark", "aria-hidden": "true", text: state === "done" ? "✓" : String(index + 1) }),
        el(
          "div",
          {},
          el("span", { class: "onboard-progress-label", text: label }),
          state === "current" && preview.step
            ? el("span", { class: "onboard-progress-step", role: "status", text: `${preview.step}${preview.detail ? ` · ${preview.detail}` : ""}` })
            : null,
          state === "current" && preview.log_url
            ? externalLink(preview.log_url, { class: "onboard-progress-log", text: "View the build log" })
            : null
        )
      );
    })
  );
}

function previewEndsAt(preview) {
  const ends = preview.expires_at ? new Date(preview.expires_at) : null;
  return ends ? `Ends ${ends.toLocaleString([], { dateStyle: "medium", timeStyle: "short" })}` : null;
}

function previewBody(status) {
  const preview = onboardPreview.status || { phase: "None" };
  const draftName = getIn(status.document, ["metadata", "name"]);
  const canDeploy = onboardOpen() && status.ready && !onboardDirty() && !onboard.busy;
  const deployHint = !onboardOpen()
    ? null
    : onboardDirty()
      ? "Validate your changes first; the test deploy uses the saved draft."
      : !status.ready
        ? "Fix the problems in the draft first."
        : null;

  if (preview.phase === "Ending") {
    return [el("p", { class: "onboard-busy", role: "status", text: `Ending the test deploy: ${preview.step || "deleting"}…` })];
  }

  if (!previewLive(preview)) {
    const parts = [
      el("p", {
        class: "onboard-hint",
        text: "Run the saved draft as a private preview for up to 48 hours before you open the pull request. It builds the latest commit on the source branch and uses the secrets you stored. Anyone signed in to tmprl-demo.cloud can open it, so you can share the link for feedback. It isn't listed in the catalog.",
      }),
    ];
    if (preview.phase === "Ended") {
      const why = preview.ended_reason === "Expired" ? " after 48 hours" : "";
      parts.push(el("p", { class: "onboard-hint", text: `Your last test deploy (${preview.project}) ended${why}.` }));
    }
    if (onboardOpen()) {
      parts.push(
        el(
          "div",
          { class: "onboard-actions" },
          el("button", {
            type: "button",
            class: "btn btn-secondary btn-sm",
            "aria-disabled": String(!canDeploy),
            title: deployHint,
            text: "Test deploy",
            onclick: () => canDeploy && deployPreview(),
          }),
          deployHint ? el("span", { class: "onboard-hint", text: deployHint }) : null
        )
      );
    }
    return parts;
  }

  const parts = [];
  if (preview.phase === "Ready") {
    parts.push(onboardAlert("success", "The test deploy is running.", "Open it below. Redeploy after you change the draft or push to the demo repo."));
  } else if (preview.phase === "Failed") {
    parts.push(onboardAlert("error", "The test deploy failed.", preview.last_error));
    if (preview.log_url) parts.push(externalLink(preview.log_url, { class: "onboard-progress-log", text: "View the build log" }));
  } else {
    parts.push(previewProgress(preview));
  }
  parts.push(
    el(
      "div",
      { class: "onboard-repo" },
      preview.url ? externalLink(preview.url, { class: "onboard-repo-name", text: preview.url.replace("https://", "") }) : null,
      preview.commit_sha ? el("span", { class: "mono", text: `${preview.commit_sha.slice(0, 7)} · revision ${preview.revision}` }) : null,
      el("span", { class: "mono", text: previewEndsAt(preview) })
    )
  );

  const renamed = draftName && preview.project && draftName !== preview.project;
  const stale = !renamed && onboardOpen() && preview.revision !== status.revision;
  if (renamed && onboardOpen()) {
    parts.push(el("p", { class: "onboard-hint", text: `This test deploy is for ${preview.project}. End it to test deploy ${draftName}.` }));
  } else if (stale) {
    parts.push(el("p", { class: "onboard-hint", text: "The draft has changed since this was deployed." }));
  }
  parts.push(
    el(
      "div",
      { class: "onboard-actions" },
      onboardOpen() && !renamed
        ? el("button", {
            type: "button",
            class: "btn btn-secondary btn-sm",
            "aria-disabled": String(!canDeploy),
            title: deployHint,
            text: "Redeploy the saved draft",
            onclick: () => canDeploy && deployPreview(),
          })
        : null,
      el("button", {
        type: "button",
        class: "btn btn-quiet btn-sm",
        "aria-disabled": String(Boolean(onboard.busy)),
        text: "End test deploy",
        onclick: () => !onboard.busy && endPreview(),
      })
    )
  );
  return parts;
}

function previewPanel(status) {
  return el("div", { "data-onboard-preview": "" }, ...previewBody(status));
}

function draftEditor(status) {
  const dirty = onboardDirty();
  const tabs = el(
    "div",
    { class: "onboard-tabs", role: "tablist" },
    ...[
      ["form", "Form"],
      ["yaml", "YAML"],
    ].map(([tab, label]) =>
      el("button", {
        type: "button",
        role: "tab",
        "aria-selected": String(onboard.tab === tab),
        text: label,
        onclick: () => switchOnboardTab(tab),
      })
    )
  );
  let body;
  if (onboard.tab === "yaml") {
    body = el("textarea", {
      class: "onboard-yaml",
      rows: 28,
      spellcheck: "false",
      text: onboard.yamlText ?? status.yaml,
      oninput: (event) => {
        onboard.yamlText = event.target.value;
        renderOnboardingActions();
      },
    });
  } else if (!onboard.working) {
    body = el("p", { class: "empty", text: "The YAML doesn't parse yet; fix it in the YAML tab." });
  } else {
    const doc = onboard.working;
    body = el(
      "div",
      { class: "onboard-form", oninput: renderOnboardingActions, onchange: renderOnboardingActions },
      catalogFields(doc),
      runtimeFields(doc),
      buildRows(doc),
      componentCards(doc),
      secretsFields(doc),
      el("p", {
        class: "onboard-hint onboard-wide",
        text: "Environment variables, serverless (Lambda) components, Worker Versioning, Nexus endpoints and extra namespaces: edit them in the YAML tab.",
      })
    );
  }
  return [
    agentPanel(status),
    tabs,
    errorList(status.errors || []),
    status.ready && !dirty ? onboardAlert("success", "The draft is valid.", "Test deploy it or open the pull request below.") : null,
    body,
    el("div", { class: "onboard-actions", "data-onboard-actions": "" }, ...draftActions(dirty)),
  ];
}

function draftActions(dirty) {
  return [
    el("button", {
      type: "button",
      class: "btn btn-primary btn-sm",
      "aria-disabled": String(!dirty || Boolean(onboard.busy)),
      text: "Validate draft",
      onclick: saveDraft,
    }),
    dirty
      ? el("button", { type: "button", class: "btn btn-quiet btn-sm", text: "Discard changes", onclick: discardDraftChanges })
      : null,
    el("span", { class: "mono", text: dirty ? "Unsaved changes" : `Revision ${onboard.status.revision}` }),
  ];
}

// Refreshes only the action bar while typing, so inputs keep focus.
function renderOnboardingActions() {
  const actions = document.querySelector("[data-onboard-actions]");
  if (actions) actions.replaceChildren(...draftActions(onboardDirty()));
  const handoff = document.querySelector("[data-onboard-handoff]");
  if (handoff) handoff.replaceChildren(...handoffBody(onboard.status));
  refreshAgentThread();
  const preview = document.querySelector("[data-onboard-preview]");
  if (preview) preview.replaceChildren(...previewBody(onboard.status));
}

function repoStep(status) {
  const repo = status && status.repo;
  if (repo && !onboard.changingRepo) {
    return onboardStep(
      1,
      "Repo",
      el(
        "div",
        { class: "onboard-repo" },
        externalLink(repo.url, { class: "onboard-repo-name", text: `${repo.owner}/${repo.name}` }),
        el("span", { class: "mono", text: `${repo.default_branch} @ ${repo.commit_sha.slice(0, 7)}` }),
        onboardOpen()
          ? el("button", {
              type: "button",
              class: "btn btn-quiet btn-sm",
              text: "Use a different repo",
              onclick: () => {
                onboard.changingRepo = true;
                renderOnboarding();
              },
            })
          : null
      )
    );
  }
  const input = el("input", {
    type: "url",
    required: true,
    placeholder: "https://github.com/temporal-sa/my-demo",
    "aria-label": "GitHub repo URL",
  });
  return onboardStep(
    1,
    "Repo",
    el("p", {
      class: "onboard-hint",
      text: "A public repo in temporal-sa, temporalio or temporal-community. The demo must follow the catalog-readiness rules in the demo buildkit.",
    }),
    el(
      "form",
      {
        class: "onboard-inline",
        onsubmit: (event) => {
          event.preventDefault();
          chooseRepo(input.value);
        },
      },
      input,
      el("button", { type: "submit", class: "btn btn-primary btn-sm", "aria-disabled": String(Boolean(onboard.busy)), text: "Check repo" }),
      onboard.changingRepo
        ? el("button", {
            type: "button",
            class: "btn btn-quiet btn-sm",
            text: "Cancel",
            onclick: () => {
              onboard.changingRepo = false;
              renderOnboarding();
            },
          })
        : null
    )
  );
}

function handoffBody(status) {
  if (onboardDirty()) {
    return [el("p", { class: "onboard-hint", text: "Validate your changes first, so the pull request has them." })];
  }
  if (!status.ready || !status.new_file_url) {
    return [el("p", { class: "onboard-hint", text: "Fix the problems in the draft first." })];
  }
  const input = el("input", {
    type: "url",
    required: true,
    placeholder: "https://github.com/temporal-sa/tmprl-demo-cloud-registry/pull/123",
    "aria-label": "Pull request URL",
  });
  return [
    el(
      "ol",
      { class: "onboard-handoff-steps" },
      el(
        "li",
        {},
        el("p", {
          text: `Open the prefilled ${status.project_path} on GitHub. Choose "Create a new branch for this commit and start a pull request", then create the pull request. Don't edit the file there: the hand-off checks it matches this draft.`,
        }),
        externalLink(status.new_file_url, { class: "btn btn-secondary btn-sm", text: "Open the file on GitHub" })
      ),
      el(
        "li",
        {},
        el("p", { text: "Paste the pull request link." }),
        el(
          "form",
          {
            class: "onboard-inline",
            onsubmit: (event) => {
              event.preventDefault();
              onboardCall("Checking pull request", [["/api/onboarding/submit", { pull_request_url: input.value }]]);
            },
          },
          input,
          el("button", { type: "submit", class: "btn btn-primary btn-sm", "aria-disabled": String(Boolean(onboard.busy)), text: "Hand off" })
        )
      )
    ),
  ];
}

function closedStep(status) {
  if (status.phase === "HandedOff") {
    return onboardStep(
      4,
      "Handed off",
      onboardAlert("success", "A catalog owner will review your pull request.", "Once it merges, the demo builds, deploys and appears in the catalog."),
      el(
        "div",
        { class: "onboard-actions" },
        externalLink(status.pull_request_url, { class: "btn btn-secondary btn-sm", text: "View pull request" }),
        el("button", {
          type: "button",
          class: "btn btn-ghost btn-sm",
          text: "Add another demo",
          onclick: () => onboardCall("Starting", [["/api/onboarding", {}]]),
        })
      )
    );
  }
  const reason = status.phase === "Expired" ? "It expired after two weeks without edits." : "It was discarded.";
  return onboardStep(
    1,
    "Your last onboarding ended",
    el("p", { class: "onboard-hint", text: reason }),
    el("button", {
      type: "button",
      class: "btn btn-primary btn-sm",
      text: "Start again",
      onclick: () => onboardCall("Starting", [["/api/onboarding", {}]]),
    })
  );
}

function renderOnboarding() {
  const root = document.querySelector("[data-onboard]");
  if (!root) return;
  if (!onboard.loaded) {
    root.replaceChildren(el("p", { class: "empty", text: "Loading…" }));
    return;
  }
  if (onboard.signInUrl) {
    root.replaceChildren(
      onboardAlert("warn", "Sign in to add a demo."),
      el("a", { class: "btn btn-primary btn-sm", href: onboard.signInUrl, text: "Sign in" })
    );
    return;
  }
  const status = onboard.status || { phase: "None" };
  const children = [];
  if (onboard.busy) children.push(el("p", { class: "onboard-busy", role: "status", text: `${onboard.busy}…` }));
  if (onboard.error) children.push(onboardAlert("error", "That didn't work.", onboard.error));
  if (status.last_error && !onboard.error && onboardOpen()) {
    children.push(onboardAlert("error", "Last check failed.", status.last_error));
  }
  if (ONBOARD_CLOSED.has(status.phase)) {
    // A test deploy outlives the onboarding, so it stays manageable here:
    // before the hand-off step, or after "Your last onboarding ended".
    const preview = onboardPreview.status || {};
    const showPreview = previewLive(preview) || preview.phase === "Ending";
    const handedOff = status.phase === "HandedOff";
    if (showPreview && handedOff) children.push(onboardStep(3, "Test deploy", previewPanel(status)));
    children.push(closedStep(status));
    if (showPreview && !handedOff) children.push(onboardStep(2, "Test deploy", previewPanel(status)));
  } else {
    children.push(repoStep(status));
    if (status.repo && !onboard.changingRepo) {
      children.push(onboardStep(2, "Configure", ...draftEditor(status)));
      children.push(onboardStep(3, "Test deploy (optional)", previewPanel(status)));
      children.push(onboardStep(4, "Open the pull request", el("div", { "data-onboard-handoff": "" }, ...handoffBody(status))));
      children.push(
        el("button", {
          type: "button",
          class: "btn btn-quiet btn-sm onboard-discard",
          text: "Discard this onboarding",
          onclick: () => {
            if (window.confirm("Discard this onboarding and its draft?")) {
              onboardCall("Discarding", [["/api/onboarding/abandon", {}]]);
            }
          },
        })
      );
    }
  }
  root.replaceChildren(...children);
  root.setAttribute("aria-busy", String(Boolean(onboard.busy)));
  markErrorFields(root, status.errors || []);
  // A full render follows an action (sending a message, applying a draft).
  scrollAgentThreadToEnd();
  scheduleOnboardPoll();
  schedulePreviewPoll();
}

// ─── Ziggy ─────────────────────────────────────────────────────────────────
// A floating helper on the demos page. Each message is one turn of the
// signed-in user's CatalogHelperWorkflow (POST /api/helper/message), which
// answers with Ziggy's reply and its view of the page: the demos to show and
// the ones to put a halo on. The view belongs to the chat, so it survives a
// reload, ends with the chat (30 idle minutes or "New chat"), and the user can
// clear it from the toolbar chip.

const ZIGGY_GREETING =
  "Hi, I'm Ziggy! Tell me who you're presenting to or what you want to show, and I'll pick demos from the catalog.";
const ZIGGY_SUGGESTIONS = [
  "An AI agent demo in Python",
  "Something for a bank or fintech",
  "Show off crash recovery",
  "Which demos have a slide deck?",
];
// Ziggy introduces himself once per browser session, shortly after the page
// loads, then the bubble fades on its own (or waits while it's hovered).
const ZIGGY_BUBBLE_KEY = "ziggy-bubble-seen";
const ZIGGY_BUBBLE_DELAY_MS = 800;
const ZIGGY_BUBBLE_MS = 7000;
const ZIGGY_BUBBLE_LINGER_MS = 2500;
const ZIGGY_MAX_CHARS = 1000;

const ziggy = {
  status: null, // the open chat's HelperStatus, or null
  open: false,
  sending: null, // the message waiting on Ziggy's reply
  error: null,
  notice: null, // why the last chat went away, shown once
  signInUrl: null,
  bubble: false,
  bubbleTimer: null,
  expiryTimer: null,
  nodes: {},
};

// Temporal's Ziggy (the official illustration, from the demo buildkit's brand
// assets). The open- and closed-eye frames share one canvas, so swapping their
// opacity reads as a blink.
function ziggyFigure() {
  return el(
    "span",
    { class: "ziggy-figure", "aria-hidden": "true" },
    el("img", { class: "ziggy-open", src: "/static/brand/ziggy-peek.png", alt: "", width: "256", height: "256" }),
    el("img", { class: "ziggy-blink", src: "/static/brand/ziggy-blink.png", alt: "", width: "256", height: "256" })
  );
}

function ziggyFace() {
  return el("img", { class: "ziggy-face", src: "/static/brand/ziggy-face.png", alt: "", width: "119", height: "128" });
}

function ziggyBubbleSeen() {
  try {
    return sessionStorage.getItem(ZIGGY_BUBBLE_KEY) === "1";
  } catch (err) {
    return false;
  }
}

function dismissZiggyBubble() {
  ziggy.bubble = false;
  clearTimeout(ziggy.bubbleTimer);
  try {
    sessionStorage.setItem(ZIGGY_BUBBLE_KEY, "1");
  } catch (err) {
    // Private windows may refuse storage; the bubble just comes back next load.
  }
}

function showZiggyBubble() {
  if (ziggy.open || ziggyBubbleSeen() || currentView() !== "demos") return;
  // Seen once it's shown: a reload in the same session doesn't repeat it.
  dismissZiggyBubble();
  ziggy.bubble = true;
  renderZiggy();
  fadeZiggyBubble(ZIGGY_BUBBLE_MS);
}

function fadeZiggyBubble(afterMs) {
  clearTimeout(ziggy.bubbleTimer);
  ziggy.bubbleTimer = setTimeout(() => {
    const bubble = ziggy.nodes.bubble;
    bubble.classList.add("is-leaving");
    bubble.addEventListener(
      "animationend",
      () => {
        bubble.classList.remove("is-leaving");
        ziggy.bubble = false;
        renderZiggy();
      },
      { once: true }
    );
  }, afterMs);
}

function holdZiggyBubble() {
  clearTimeout(ziggy.bubbleTimer);
  ziggy.nodes.bubble.classList.remove("is-leaving");
}

function setupZiggy() {
  const root = document.querySelector("[data-ziggy]");
  const textarea = el("textarea", {
    rows: "1",
    maxlength: String(ZIGGY_MAX_CHARS),
    placeholder: "Describe your audience or use case…",
    "aria-label": "Message Ziggy",
    oninput: () => {
      textarea.style.height = "auto";
      textarea.style.height = `${textarea.scrollHeight}px`;
    },
    onkeydown: (event) => {
      if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
        event.preventDefault();
        sendZiggy(textarea.value);
      }
    },
  });
  const send = el("button", { type: "submit", class: "btn btn-primary btn-sm", text: "Send" });
  const composer = el(
    "form",
    {
      class: "ziggy-composer",
      onsubmit: (event) => {
        event.preventDefault();
        sendZiggy(textarea.value);
      },
    },
    textarea,
    send,
    el("p", { class: "ziggy-note", text: "Chats clear after 30 minutes without a message." })
  );
  const thread = el("div", { class: "ziggy-thread", role: "log", "aria-live": "polite" });
  const signin = el("div", { class: "ziggy-signin" });
  const newChat = el("button", {
    type: "button",
    class: "btn btn-quiet btn-sm",
    text: "New chat",
    onclick: newZiggyChat,
  });
  const panel = el(
    "section",
    {
      class: "ziggy-panel",
      role: "dialog",
      "aria-label": "Chat with Ziggy",
      onkeydown: (event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          closeZiggy();
        }
      },
    },
    el(
      "header",
      { class: "ziggy-head" },
      ziggyFace(),
      el(
        "div",
        { class: "ziggy-head-title" },
        el("strong", { text: "Ziggy" }),
        el("span", { text: "Helps you pick a demo" })
      ),
      newChat,
      el("button", { type: "button", class: "icon-btn", "aria-label": "Close chat", onclick: closeZiggy }, icon("close"))
    ),
    thread,
    signin,
    composer
  );
  const bubble = el(
    "div",
    {
      class: "ziggy-bubble",
      role: "button",
      tabindex: "0",
      onclick: openZiggy,
      // Reading it keeps it up; it fades a moment after the pointer leaves.
      onmouseenter: holdZiggyBubble,
      onfocusin: holdZiggyBubble,
      onmouseleave: () => ziggy.bubble && fadeZiggyBubble(ZIGGY_BUBBLE_LINGER_MS),
      onfocusout: () => ziggy.bubble && fadeZiggyBubble(ZIGGY_BUBBLE_LINGER_MS),
      onkeydown: (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          openZiggy();
        }
      },
    },
    "Hi, I'm Ziggy! Need help picking a demo?",
    el("strong", { text: "Talk to me!" }),
    el(
      "button",
      {
        type: "button",
        class: "ziggy-bubble-close",
        "aria-label": "Dismiss",
        onclick: (event) => {
          event.stopPropagation();
          dismissZiggyBubble();
          renderZiggy();
        },
      },
      icon("close")
    )
  );
  const badge = el("span", { class: "ziggy-launcher-badge", "aria-hidden": "true" });
  const launcher = el(
    "button",
    {
      type: "button",
      class: "ziggy-launcher",
      "aria-label": "Chat with Ziggy, the demo picker",
      "aria-expanded": "false",
      onclick: () => (ziggy.open ? closeZiggy() : openZiggy()),
    },
    ziggyFigure(),
    badge
  );
  root.replaceChildren(panel, el("div", { class: "ziggy-dock" }, bubble, launcher));
  ziggy.nodes = { root, panel, thread, signin, composer, textarea, send, newChat, bubble, badge, launcher };

  document.querySelector("[data-ziggy-chip]").addEventListener("click", () => {
    clearZiggyView();
    onDemosFilterChange();
  });
  setTimeout(showZiggyBubble, ZIGGY_BUBBLE_DELAY_MS);
  renderZiggy();
}

function ziggySignInUrl() {
  return `/auth/login?return_to=${encodeURIComponent(window.location.href)}`;
}

async function loadZiggy() {
  try {
    const res = await fetch("/api/helper", { credentials: "same-origin" });
    if (res.status === 401) ziggy.signInUrl = ziggySignInUrl();
    else if (res.ok) acceptZiggyStatus(await res.json());
  } catch (err) {
    // Ziggy stays clickable; sending a message reports what's wrong.
  }
  renderZiggy();
}

function acceptZiggyStatus(status) {
  clearTimeout(ziggy.expiryTimer);
  if (!status || status.phase !== "Open") {
    ziggy.status = null;
    applyZiggyView(null);
    return;
  }
  ziggy.status = status;
  applyZiggyView(status.view);
  // The workflow ends the chat after 30 idle minutes; drop it here too, so a
  // page left open doesn't keep showing a filter from a chat that's gone.
  const remaining = Date.parse(status.expires_at) - Date.now();
  if (remaining > 0) ziggy.expiryTimer = setTimeout(expireZiggy, remaining + 1000);
}

function expireZiggy() {
  if (ziggy.sending) return;
  ziggy.notice = "Our last chat timed out after 30 minutes, so I cleared it. Ask me anything to start again.";
  acceptZiggyStatus(null);
  renderZiggy();
}

function sameList(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function applyZiggyView(view) {
  const known = new Set(state.demos.map((demo) => demo.id));
  const ids = view && Array.isArray(view.demo_ids) ? view.demo_ids.filter((id) => known.has(id)) : [];
  const picks = ids.length ? ids : null;
  const highlights = ((view && view.highlights) || []).filter((id) => known.has(id));
  const picksChanged = !sameList(picks, state.ziggyPicks);
  if (!picksChanged && sameList(highlights, [...state.ziggyHighlights])) return;
  state.ziggyPicks = picks;
  state.ziggyHighlights = new Set(highlights);
  if (picksChanged && picks) {
    // Show exactly what Ziggy picked; the user's own filters would hide some.
    state.demosSearch = "";
    state.selectedTags.clear();
    state.selectedSdks.clear();
    state.selectedDemoAiSdks.clear();
  }
  if (currentView() === "demos") onDemosFilterChange();
  renderZiggyLauncher();
}

function clearZiggyView() {
  if (!state.ziggyPicks && !state.ziggyHighlights.size) return;
  state.ziggyPicks = null;
  state.ziggyHighlights = new Set();
  renderZiggyLauncher();
  if (!ziggy.status) return;
  ziggy.status.view = { demo_ids: null, highlights: [] };
  // Tell the chat, so a reload doesn't bring the filter back and Ziggy knows.
  postJSON("/api/helper/clear-view", {})
    .then((status) => {
      if (!ziggy.sending) acceptZiggyStatus(status);
    })
    .catch(() => {});
}

function ziggyPickClass(base, demo) {
  return state.ziggyHighlights.has(demo.id) ? `${base} is-ziggy-pick` : base;
}

function ziggyTag(demo) {
  if (!state.ziggyHighlights.has(demo.id)) return [];
  return [el("span", { class: "tag tag-ziggy", title: "Ziggy suggests starting here" }, icon("sparkle"), "Ziggy's pick")];
}

function renderZiggyChip() {
  const chip = document.querySelector("[data-ziggy-chip]");
  const picks = state.ziggyPicks;
  const highlighted = state.ziggyHighlights.size;
  chip.hidden = !picks && !highlighted;
  if (chip.hidden) return;
  const label = picks ? `Ziggy's picks · ${picks.length}` : `Ziggy highlighted ${highlighted}`;
  chip.replaceChildren(document.createTextNode(label), icon("close"));
  chip.title = picks ? "Showing only the demos Ziggy picked. Click to show every demo." : "Click to remove Ziggy's highlights.";
  chip.setAttribute("aria-label", `${label}. Clear`);
}

function renderZiggyLauncher() {
  const { root, badge } = ziggy.nodes;
  if (!root) return;
  root.hidden = currentView() !== "demos";
  const picks = state.ziggyPicks ? state.ziggyPicks.length : 0;
  badge.hidden = ziggy.open || !picks;
  badge.textContent = String(picks);
}

function openZiggy() {
  ziggy.open = true;
  dismissZiggyBubble();
  renderZiggy();
  if (!ziggy.signInUrl) ziggy.nodes.textarea.focus();
}

function closeZiggy() {
  ziggy.open = false;
  renderZiggy();
  ziggy.nodes.launcher.focus();
}

async function sendZiggy(raw) {
  const text = raw.trim();
  if (!text || ziggy.sending) return;
  ziggy.sending = text;
  ziggy.error = null;
  ziggy.notice = null;
  ziggy.nodes.textarea.value = "";
  ziggy.nodes.textarea.style.height = "auto";
  renderZiggy();
  try {
    acceptZiggyStatus(await postJSON("/api/helper/message", { text }));
  } catch (err) {
    if (err.status === 401) {
      ziggy.signInUrl = ziggySignInUrl();
    } else {
      ziggy.error = err.message;
      if (!ziggy.nodes.textarea.value) ziggy.nodes.textarea.value = text;
    }
  } finally {
    ziggy.sending = null;
    renderZiggy();
  }
}

async function newZiggyChat() {
  if (ziggy.sending) return;
  ziggy.error = null;
  ziggy.notice = null;
  if (ziggy.status) {
    try {
      await postJSON("/api/helper/new", {});
    } catch (err) {
      ziggy.error = err.message;
      renderZiggy();
      return;
    }
  }
  acceptZiggyStatus(null);
  renderZiggy();
  ziggy.nodes.textarea.focus();
}

// Ziggy's replies are light markdown: paragraphs, bullet or numbered lists,
// **bold**, *italic*, `code` and [links](https://…). It builds DOM nodes, never
// HTML, so a reply can't inject markup.
const ZIGGY_INLINE = /\*\*(.+?)\*\*|`([^`]+)`|\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)|(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])/g;

function ziggyInline(text) {
  const nodes = [];
  let last = 0;
  for (const match of text.matchAll(ZIGGY_INLINE)) {
    if (match.index > last) nodes.push(text.slice(last, match.index));
    const [, bold, code, label, href, italic] = match;
    if (bold !== undefined) nodes.push(el("strong", {}, ...ziggyInline(bold)));
    else if (code !== undefined) nodes.push(el("code", { text: code }));
    else if (label !== undefined) nodes.push(el("a", { href, target: "_blank", rel: "noopener noreferrer", text: label }));
    else nodes.push(el("em", {}, ...ziggyInline(italic)));
    last = match.index + match[0].length;
  }
  if (last < text.length) nodes.push(text.slice(last));
  return nodes;
}

function ziggyMarkdown(text) {
  const blocks = [];
  let list = null;
  let paragraph = [];
  const flush = () => {
    if (paragraph.length) blocks.push(el("p", {}, ...ziggyInline(paragraph.join("\n"))));
    paragraph = [];
  };
  for (const line of text.split("\n")) {
    const item = line.match(/^\s*(?:([-*•])|(\d+)[.)])\s+(.*)$/);
    if (item) {
      flush();
      const tag = item[1] ? "ul" : "ol";
      if (!list || list.tagName.toLowerCase() !== tag) blocks.push((list = el(tag)));
      list.appendChild(el("li", {}, ...ziggyInline(item[3])));
    } else if (!line.trim()) {
      flush();
      list = null;
    } else {
      list = null;
      paragraph.push(line.replace(/^#+\s+/, ""));
    }
  }
  flush();
  return blocks;
}

function ziggyMessageView(message) {
  if (message.role === "user") return el("div", { class: "ziggy-msg is-user", text: message.text });
  const failed = message.outcome !== "Done";
  const actions = message.actions || [];
  return el(
    "div",
    { class: `ziggy-msg is-ziggy${failed ? " is-failed" : " ziggy-md"}` },
    ...(failed ? [`Sorry, I couldn't answer that: ${message.text}`] : ziggyMarkdown(message.text)),
    actions.length
      ? el("div", { class: "ziggy-actions" }, ...actions.map((action) => el("span", { text: action })))
      : null
  );
}

function renderZiggyThread() {
  const messages = (ziggy.status && ziggy.status.messages) || [];
  const items = [el("div", { class: "ziggy-msg is-ziggy", text: ZIGGY_GREETING })];
  if (ziggy.notice) items.push(el("p", { class: "ziggy-note", text: ziggy.notice }));
  items.push(...messages.map(ziggyMessageView));
  if (ziggy.sending) {
    items.push(
      el("div", { class: "ziggy-msg is-user", text: ziggy.sending }),
      el(
        "div",
        { class: "ziggy-msg is-ziggy ziggy-typing", role: "status", "aria-label": "Ziggy is typing" },
        el("i"),
        el("i"),
        el("i")
      )
    );
  } else if (!messages.length && !ziggy.signInUrl) {
    items.push(
      el(
        "div",
        { class: "ziggy-suggestions" },
        ...ZIGGY_SUGGESTIONS.map((text) =>
          el("button", { type: "button", text, onclick: () => sendZiggy(text) })
        )
      )
    );
  }
  if (ziggy.error) items.push(el("p", { class: "ziggy-error", role: "alert", text: ziggy.error }));
  ziggy.nodes.thread.replaceChildren(...items);
  ziggy.nodes.thread.scrollTop = ziggy.nodes.thread.scrollHeight;
}

function renderZiggy() {
  const n = ziggy.nodes;
  if (!n.root) return;
  renderZiggyLauncher();
  n.panel.hidden = !ziggy.open;
  n.bubble.hidden = ziggy.open || !ziggy.bubble;
  n.launcher.setAttribute("aria-expanded", ziggy.open ? "true" : "false");
  if (!ziggy.open) return;
  renderZiggyThread();
  n.composer.hidden = !!ziggy.signInUrl;
  n.signin.hidden = !ziggy.signInUrl;
  if (ziggy.signInUrl) {
    n.signin.replaceChildren(
      el("p", { text: "Sign in with your Temporal Google account to chat with me." }),
      el("a", { class: "btn btn-primary btn-sm", href: ziggy.signInUrl, text: "Sign in" })
    );
  }
  n.send.setAttribute("aria-disabled", ziggy.sending ? "true" : "false");
  n.newChat.hidden = !ziggy.status || !!ziggy.sending;
}

// ─── Boot ─────────────────────────────────────────────────────────────────

async function init() {
  loadFiltersFromUrl();
  setupTabs();
  setupKeyboard();
  setupDrawer();
  setupDemosFilters();
  setupWorkshopsFilters();
  setupNamespaceAccess();
  setupZiggy();
  await loadData();
  render();
  syncDrawerToHash();
  loadZiggy();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
