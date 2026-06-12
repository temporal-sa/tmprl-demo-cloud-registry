const projectGrid = document.querySelector("[data-project-grid]");
const searchInput = document.querySelector("[data-search]");
const phaseSelect = document.querySelector("[data-phase]");
const projectCount = document.querySelector("[data-project-count]");
const liveStatusCount = document.querySelector("[data-live-status-count]");
const authCount = document.querySelector("[data-auth-count]");

const state = {
  catalog: null,
  search: "",
  phase: "all",
};

function text(value, fallback = "") {
  return typeof value === "string" && value.trim() ? value : fallback;
}

function projectPhase(project) {
  return text(project.status?.phase, "Catalogued");
}

function statusClass(phase) {
  return phase.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function matches(project) {
  const phase = projectPhase(project);
  if (state.phase !== "all" && phase !== state.phase) {
    return false;
  }

  const query = state.search.trim().toLowerCase();
  if (!query) {
    return true;
  }

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

  return haystack.includes(query);
}

function renderStats(projects, catalog) {
  projectCount.textContent = String(projects.length);
  liveStatusCount.textContent = String(catalog?.liveStatusCount ?? 0);
  authCount.textContent = String(projects.filter((project) => project.authRequired).length);
}

function renderPhaseOptions(projects) {
  const phases = [...new Set(projects.map(projectPhase))].sort();
  phaseSelect.replaceChildren(
    new Option("All phases", "all"),
    ...phases.map((phase) => new Option(phase, phase)),
  );
  phaseSelect.value = state.phase;
}

function renderProjects(projects) {
  const visible = projects.filter(matches);
  if (visible.length === 0) {
    renderEmpty("No matching projects.");
    return;
  }

  projectGrid.replaceChildren(...visible.map(renderProject));
}

function renderEmpty(message) {
  const empty = document.createElement("div");
  empty.className = "empty";
  empty.textContent = message;
  projectGrid.replaceChildren(empty);
}

function renderProject(project) {
  const phase = projectPhase(project);
  const article = document.createElement("article");
  article.className = "project";

  const body = document.createElement("div");
  const title = document.createElement("h2");
  title.className = "project-title";
  title.textContent = text(project.title, project.name);

  const summary = document.createElement("p");
  summary.className = "project-summary";
  summary.textContent = text(project.status?.message, text(project.summary, "No summary provided."));

  const tags = document.createElement("div");
  tags.className = "tags";
  for (const tag of project.tags ?? []) {
    const chip = document.createElement("span");
    chip.className = "tag";
    chip.textContent = tag;
    tags.append(chip);
  }

  body.append(title, summary, tags);

  const meta = document.createElement("aside");
  meta.className = "meta";

  const status = document.createElement("span");
  status.className = `status ${statusClass(phase)}`;
  status.textContent = phase;

  const links = document.createElement("div");
  links.className = "links";
  appendLink(links, "App", project.appUrl);
  appendLink(links, "Source", project.sourceRepo);

  const namespace = document.createElement("div");
  namespace.className = "detail";
  namespace.textContent = `Temporal namespace: ${text(project.temporalNamespace, "unassigned")}`;

  const sdks = document.createElement("div");
  sdks.className = "detail";
  sdks.textContent = `SDKs: ${(project.temporalSdks ?? []).join(", ") || "unspecified"}`;

  meta.append(status, links, namespace, sdks);
  article.append(body, meta);
  return article;
}

function appendLink(parent, label, url) {
  if (!url) {
    return;
  }
  const link = document.createElement("a");
  link.href = url;
  link.textContent = label;
  link.rel = "noreferrer";
  parent.append(link);
}

async function loadCatalog() {
  const response = await fetch("/api/catalog", { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Catalog request failed with ${response.status}`);
  }
  state.catalog = await response.json();
  const projects = state.catalog.projects ?? [];
  renderStats(projects, state.catalog.catalog);
  renderPhaseOptions(projects);
  renderProjects(projects);
}

searchInput.addEventListener("input", (event) => {
  state.search = event.target.value;
  renderProjects(state.catalog?.projects ?? []);
});

phaseSelect.addEventListener("change", (event) => {
  state.phase = event.target.value;
  renderProjects(state.catalog?.projects ?? []);
});

loadCatalog().catch((error) => {
  renderEmpty(`Catalog unavailable: ${error.message}`);
});
