// Demo Registry — vanilla JS SPA.
// Two views over the same catalog JSON:
//   /         → demos (canonical feature cards + a grid of everything else;
//               each demo opens in a detail drawer, deep-linked as /#<id>)
//   /courses  → Instruqt workshops (the server routes this path to the SPA)

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

const state = {
  demos: [],
  courses: [],
  route: window.location.pathname,
  demosSearch: "",
  selectedTags: new Set(),
  selectedSdks: new Set(),
  workshopsSearch: "",
  selectedWorkshopSdks: new Set(),
  openDemoId: null,
};

const ICONS = {
  external: '<path d="M7 4h9v9"/><path d="M16 4 5 15"/>',
  lock: '<rect x="4" y="9" width="12" height="8" rx="2"/><path d="M7 9V6a3 3 0 0 1 6 0v3"/>',
  bolt: '<path d="M11 2 4 11h5l-1 7 7-9h-5z"/>',
  star: '<path d="m10 2.5 2.3 4.8 5.2.7-3.8 3.6.9 5.2L10 14.3l-4.6 2.5.9-5.2L2.5 8l5.2-.7z"/>',
  link: '<path d="M8.5 11.5a3.5 3.5 0 0 0 5 0l2.5-2.5a3.5 3.5 0 0 0-5-5L10 5"/><path d="M11.5 8.5a3.5 3.5 0 0 0-5 0L4 11a3.5 3.5 0 0 0 5 5l1-1"/>',
  close: '<path d="m5 5 10 10M15 5 5 15"/>',
  arrow: '<path d="M4 10h12M11 5l5 5-5 5"/>',
  sparkle: '<path d="M10 1c.8 5.2 3.8 8.2 9 9-5.2.8-8.2 3.8-9 9-.8-5.2-3.8-8.2-9-9 5.2-.8 8.2-3.8 9-9Z"/>',
  terminal: '<rect x="2.5" y="3.5" width="15" height="13"/><path d="m6 8 2.5 2L6 12m4.5 0H14"/>',
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

function sdkList(sdks) {
  if (!sdks.length) return null;
  return el(
    "div",
    { class: "sdk-list" },
    ...sdks.map((sdk) => el("span", { class: "sdk" }, sdkMark(sdk), sdk))
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
  return state.route === WORKSHOPS_ROUTE ? "workshops" : "demos";
}

function setRoute(target) {
  if (target === state.route) return;
  closeDrawer({ updateHash: false });
  state.route = target;
  const search = target === WORKSHOPS_ROUTE ? workshopsQueryString() : demosQueryString();
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
    if (event.key === "Escape" && state.openDemoId) {
      closeDrawer();
      return;
    }
    const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName);
    if (event.key === "/" && !typing && !state.openDemoId) {
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
  } else {
    state.demosSearch = params.get("q") || "";
    state.selectedTags = splitParam(params, "tags");
    state.selectedSdks = splitParam(params, "sdks");
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
  ]);
}

function workshopsQueryString() {
  return buildQuery([
    ["q", state.workshopsSearch.trim()],
    ["sdks", [...state.selectedWorkshopSdks].join(",")],
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
  return !!(state.selectedTags.size || state.selectedSdks.size || state.demosSearch.trim());
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
    onDemosFilterChange();
  });
  const menu = document.querySelector("[data-topics-menu]");
  document.addEventListener("click", (event) => {
    if (menu.open && !menu.contains(event.target)) menu.open = false;
  });
}

function renderDemosFilters() {
  document.querySelector("[data-demos-search]").value = state.demosSearch;

  const sdks = [...countBy(state.demos, (d) => d.sdks).keys()].sort();
  document
    .querySelector("[data-demos-sdks]")
    .replaceChildren(...sdks.map((sdk) => sdkChip(sdk, state.selectedSdks, onDemosFilterChange)));

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
}

function demoMatchesFilters(demo) {
  const q = state.demosSearch.trim().toLowerCase();
  const matchesTags =
    state.selectedTags.size === 0 ||
    [...state.selectedTags].every((t) => demo.tags.includes(t));
  const matchesSdks =
    state.selectedSdks.size === 0 ||
    [...state.selectedSdks].some((s) => demo.sdks.includes(s));
  const matchesSearch =
    !q ||
    demo.name.toLowerCase().includes(q) ||
    demo.id.toLowerCase().includes(q) ||
    demo.summary.toLowerCase().includes(q) ||
    demo.tags.some((t) => t.toLowerCase().includes(q)) ||
    demo.sdks.some((s) => s.toLowerCase().includes(q));
  return matchesTags && matchesSdks && matchesSearch;
}

function renderDemosList() {
  const canonical = state.demos.filter((demo) => demo.canonical);
  const others = state.demos.filter((demo) => !demo.canonical);
  const visibleCanonical = canonical.filter(demoMatchesFilters);
  const visibleOthers = others.filter(demoMatchesFilters);

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

function renderFeatureCard(demo) {
  const nt = demo.nonTechnicalAssets;
  const assetChecklist = [
    ["Speaker notes", nt.speakerNotes],
    ["Slide deck", nt.slideDeck],
    ["Golden path recording", nt.goldenPath],
    ["External recording", nt.externalRecording],
    ["Runbook", demo.technicalAssets.runbook],
    ["Repository", demo.technicalAssets.repo],
  ];
  return el(
    "article",
    { class: "feature", id: `demo-${demo.id}` },
    demoArt(demo),
    el(
      "div",
      { class: "feature-body" },
      el("p", { class: "eyebrow", text: "Canonical demo" }),
      el("h3", { text: demo.name }),
      el("p", { class: "feature-summary", text: demo.summary }),
      sdkList(demo.sdks),
      tagRow([...flagTags(demo), ...topicTags(demo.tags)]),
      el(
        "div",
        { class: "feature-actions" },
        openAppButton(demo, "btn btn-primary", "Open demo") ||
          el("span", { class: "btn btn-primary", "aria-disabled": "true", text: "No app URL" }),
        nt.speakerNotes
          ? externalLink(nt.speakerNotes, { class: "btn btn-secondary" }, "Speaker notes")
          : null,
        namespaceButton(demo, "btn btn-ghost"),
        el(
          "button",
          { type: "button", class: "btn btn-quiet", onclick: () => openDrawer(demo.id) },
          "All details",
          icon("arrow")
        )
      ),
      el(
        "div",
        { class: "checklist", role: "list", "aria-label": "Presenter assets" },
        ...assetChecklist.map(([label, url]) =>
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
    { class: "card", id: `demo-${demo.id}`, style: `--sdk-bar:${sdkBar(demo.sdks)}` },
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
    tagRow([...flagTags(demo), ...topicTags(demo.tags, 3)]),
    el("div", { class: "card-foot" }, sdkList(demo.sdks), cardActions(demo))
  );
}

function cardActions(demo) {
  const actions = [openAppButton(demo), namespaceButton(demo)].filter(Boolean);
  return actions.length ? el("div", { class: "card-actions" }, ...actions) : null;
}

// ─── Demo drawer ───────────────────────────────────────────────────────────

let drawerReturnFocus = null;

function findDemo(id) {
  return state.demos.find((demo) => demo.id === id) || null;
}

function openDrawer(id, { updateHash = true } = {}) {
  const demo = findDemo(id);
  if (!demo) return;
  if (!state.openDemoId) drawerReturnFocus = document.activeElement;
  state.openDemoId = id;
  if (updateHash && window.location.hash !== `#${id}`) {
    window.history.pushState(null, "", `${window.location.pathname}${window.location.search}#${id}`);
  }
  const drawer = document.querySelector("[data-drawer]");
  const panel = document.querySelector("[data-drawer-panel]");
  clearAllWorkspaceTimers();
  panel.replaceChildren(...renderDrawerContent(demo));
  panel.scrollTop = 0;
  drawer.hidden = false;
  document.body.classList.add("drawer-open");
  panel.focus();
}

function closeDrawer({ updateHash = true } = {}) {
  if (!state.openDemoId) return;
  state.openDemoId = null;
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
  if (id && currentView() === "demos" && findDemo(id)) {
    if (state.openDemoId !== id) openDrawer(id, { updateHash: false });
  } else {
    closeDrawer({ updateHash: false });
  }
}

function setupDrawer() {
  document.querySelector("[data-drawer-close]").addEventListener("click", () => closeDrawer());
}

function copyLink(id) {
  const url = `${window.location.origin}/#${id}`;
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
    sdkList(demo.sdks),
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
      demo.crashable ? crashableWorkspaceCard(demo) : null,
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

function crashableWorkspaceCard(demo) {
  const container = el("section", {
    class: "panel panel-crash",
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
  clearWorkspaceTimers(demo.id);
  const status = workspaceState.get(demo.id) || null;
  const phase = status
    ? status.phase
    : workspaceFetching.has(demo.id)
    ? "Loading"
    : "None";

  const children = [el("h3", { class: "panel-title", text: "Crashable environment" })];
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

  if (phase === "Loading") {
    children.push(note("Checking for an existing environment…"));
  } else if (phase === "None") {
    children.push(
      note(
        "Spin up a private, time-limited copy of this demo’s Kubernetes workers that you can crash or scale down to watch Temporal recover. Auto-deletes after 1 hour."
      ),
      el(
        "div",
        { class: "ws-actions" },
        workspaceButton("Create crashable environment", "btn-primary", () =>
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
    children.push(
      el("span", { class: "ws-live", text: "Running" }),
      el(
        "div",
        { class: "ws-actions", style: "margin-top:0" },
        appUrl ? externalLink(appUrl, { class: "btn btn-sm btn-primary" }, "Open environment", icon("external")) : null,
        workspaceButton("Crash workers", "btn-pink", () =>
          workspaceAction(demo, "/api/crashable-workspace/crash")
        ),
        workspaceButton("Scale down workers", "btn-secondary", () =>
          workspaceAction(demo, "/api/crashable-workspace/scale-down")
        ),
        extend(),
        remove()
      ),
      ...trailer()
    );
    setWorkspaceTimer(demo.id, armWorkspaceCountdown(demo, expiresAt));
  } else if (phase === "ScaledDown") {
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
  return !!(state.selectedWorkshopSdks.size || state.workshopsSearch.trim());
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
    onWorkshopsFilterChange();
  });
}

function courseSdks(course) {
  return Array.isArray(course.temporalSdks) ? course.temporalSdks : [];
}

function renderWorkshopsFilters() {
  document.querySelector("[data-workshops-search]").value = state.workshopsSearch;
  const sdks = [...countBy(state.courses, courseSdks).keys()].sort();
  document
    .querySelector("[data-workshops-sdks]")
    .replaceChildren(
      ...sdks.map((sdk) => sdkChip(sdk, state.selectedWorkshopSdks, onWorkshopsFilterChange))
    );
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
// a single card with one launch button per variant.
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

function filteredCourseGroups() {
  const q = state.workshopsSearch.trim().toLowerCase();
  return groupCourses(state.courses).filter(
    (members) =>
      (!q || members.some((course) => courseHaystack(course).includes(q))) &&
      (!state.selectedWorkshopSdks.size || members.some(courseMatchesSdk))
  );
}

function uniqueValues(values) {
  return [...new Set(values.filter(Boolean))];
}

function courseResources(members) {
  const seen = new Set();
  const resources = [];
  for (const course of members) {
    for (const resource of Array.isArray(course.resources) ? course.resources : []) {
      if (!resource || !resource.url || seen.has(resource.url)) continue;
      seen.add(resource.url);
      resources.push(resource);
    }
  }
  return resources;
}

function renderWorkshopCard(members) {
  const representative =
    members.find((course) => course.name === course.parent) ||
    members.slice().sort((a, b) => a.name.localeCompare(b.name))[0];
  const sdks = uniqueValues(members.flatMap(courseSdks));
  const tags = uniqueValues(members.flatMap((course) => course.tags || [])).filter(
    (tag) => tag.toLowerCase() !== "workshop"
  );
  const launchable = members.filter((course) => course.trackUrl);

  // Every workshop lists its tracks the same way, even when there is only one,
  // so a newly added language variant simply shows up as another row.
  const launch = launchable.length
    ? el(
        "div",
        {},
        el("span", { class: "launch-label", text: "Choose a track" }),
        el(
          "div",
          { class: "track-list" },
          ...launchable.map((course) => {
            const label = trackLabel(course);
            const sdk = courseSdks(course)[0];
            return externalLink(
              course.trackUrl,
              {
                class: courseMatchesSdk(course) ? "is-match" : null,
                "aria-label": label
                  ? `Launch the ${label} track on Instruqt`
                  : `Launch ${representative.title || course.name} on Instruqt`,
              },
              sdk ? sdkMark(sdk) : null,
              label || "Launch track",
              icon("arrow")
            );
          })
        )
      )
    : el("span", { class: "launch-label", text: "No track published yet" });

  const workshopIcon = icon("terminal");
  workshopIcon.setAttribute("class", "workshop-icon");
  const resources = courseResources(members);
  return el(
    "article",
    { class: "workshop" },
    workshopIcon,
    el("p", {
      class: "eyebrow",
      text: launchable.length
        ? `Instruqt workshop · ${plural(launchable.length, "track")}`
        : "Instruqt workshop",
    }),
    el("h3", { text: representative.title || representative.name }),
    el("p", {
      class: "workshop-summary",
      text: representative.summary || "A hands-on Temporal workshop.",
    }),
    sdkList(sdks),
    tagRow(topicTags(tags)),
    el("div", { class: "workshop-launch" }, launch),
    resources.length
      ? el(
          "div",
          { class: "workshop-resources" },
          ...resources.map((resource) =>
            externalLink(
              resource.url,
              { title: resource.kind || resource.label },
              `${resource.label} ↗`
            )
          )
        )
      : null
  );
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
  document.title =
    view === "workshops" ? "Workshops · Temporal Field Kit" : "Demos · Temporal Field Kit";
  renderCounts();
  if (view === "workshops") {
    renderWorkshopsFilters();
    renderWorkshopsList();
  } else {
    renderDemosFilters();
    renderDemosList();
  }
}

// ─── Boot ─────────────────────────────────────────────────────────────────

async function init() {
  loadFiltersFromUrl();
  setupTabs();
  setupKeyboard();
  setupDrawer();
  setupDemosFilters();
  setupWorkshopsFilters();
  await loadData();
  render();
  syncDrawerToHash();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
