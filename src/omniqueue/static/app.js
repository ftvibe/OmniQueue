/* OmniQueue dashboard: polls /api/state and renders clusters + jobs. No dependencies. */
(() => {
  "use strict";

  const POLL_MS = 5000;
  const TOKEN = document.querySelector('meta[name="omniqueue-token"]')?.content || "";
  const post = (url) => fetch(url, { method: "POST", headers: { "X-OmniQueue-Token": TOKEN } });
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const prefs = loadPrefs();
  const state = {
    snapshot: null,
    tab: prefs.tab || "all",
    sort: prefs.sort || { key: "submit_time", desc: true },
    search: "",
    hiddenClusters: new Set(prefs.hiddenClusters || []),
    windowHours: prefs.windowHours ?? 72,
    projectsCollapsed: !!prefs.projectsCollapsed,
    selected: null,
    error: null,
    matches: new Map(), // job key -> fuzzy match info for the current search
    expanded: new Set(), // array groups opened to show their tasks
    view: "jobs", // "jobs" | "load" | "predict" | "project"; the others are entered explicitly
    projectKey: null, // "cluster/project" shown in the project view
    load: null, // last /api/load answer
    loadTimer: null,
    projects: null, // last /api/projects answer (slow background poll, own card row)
    projTimer: null,
    experimental: (() => { try { return localStorage.getItem("omniqueue.experimental") === "1"; } catch { return false; } })(),
    prediction: null,
  };

  // ---------- helpers ----------
  function loadPrefs() {
    try { return JSON.parse(localStorage.getItem("omniqueue.prefs") || "{}"); } catch { return {}; }
  }
  function savePrefs() {
    try {
      localStorage.setItem("omniqueue.prefs", JSON.stringify({
        tab: state.tab, sort: state.sort, hiddenClusters: [...state.hiddenClusters], windowHours: state.windowHours,
        projectsCollapsed: state.projectsCollapsed,
      }));
    } catch { /* ignore */ }
  }
  const pad = (n) => String(n).padStart(2, "0");
  function fmtDuration(s) {
    if (s === null || s === undefined) return "–";
    s = Math.max(0, Math.round(s));
    const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
    if (d) return `${d}d ${pad(h)}:${pad(m)}`;
    return `${pad(h)}:${pad(m)}:${pad(sec)}`;
  }
  function parseSlurmTime(t) {
    // Slurm prints local cluster time without a zone; treat it as browser local.
    if (!t) return null;
    const d = new Date(t.replace(" ", "T"));
    return isNaN(d) ? null : d;
  }
  function fmtTime(t) {
    const d = parseSlurmTime(t);
    if (!d) return "–";
    const now = new Date();
    const sameDay = d.toDateString() === now.toDateString();
    const hm = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
    if (sameDay) return hm;
    return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${hm}`;
  }
  function clock(unix) {
    if (!unix) return "never";
    const d = new Date(unix * 1000);
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  function ago(unix) {
    if (!unix) return "never";
    const s = Math.round(Date.now() / 1000 - unix);
    if (s < 5) return "just now";
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
    return `${Math.floor(s / 86400)}d ago`;
  }
  function el(tag, attrs = {}, ...children) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") e.className = v;
      else if (k === "style") e.style.cssText = v;
      else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
      else if (v !== null && v !== undefined) e.setAttribute(k, v);
    }
    for (const c of children.flat()) if (c !== null && c !== undefined) e.append(c.nodeType ? c : String(c));
    return e;
  }
  const STATE_LABEL = {
    RUNNING: "running", PENDING: "pending", COMPLETED: "completed", FAILED: "failed", TIMEOUT: "timeout",
    OUT_OF_MEMORY: "out of memory", CANCELLED: "cancelled", NODE_FAIL: "node fail", COMPLETING: "completing",
    CONFIGURING: "configuring", SUSPENDED: "suspended", PREEMPTED: "preempted", REQUEUED: "requeued",
    VANISHED: "vanished", BOOT_FAIL: "boot fail", DEADLINE: "deadline",
  };
  const stateLabel = (s) => STATE_LABEL[s] || s.toLowerCase().replace(/_/g, " ");

  // ---------- fuzzy matching ----------
  // fzf-style subsequence match: every query character must appear in order.
  // Scores favour consecutive runs, matches at word starts, and short targets.
  function fuzzyMatch(query, text) {
    if (!query) return { score: 0, positions: [] };
    if (!text) return null;
    const q = query.toLowerCase(), t = text.toLowerCase();
    const exact = t.indexOf(q);
    if (exact >= 0) {
      const positions = Array.from({ length: q.length }, (_, i) => exact + i);
      const atStart = exact === 0 || /[^a-z0-9]/.test(t[exact - 1]);
      return { score: 1000 + (atStart ? 200 : 0) + q.length * 20 - t.length, positions };
    }
    const positions = [];
    let ti = 0, score = 0, prev = -2;
    for (let qi = 0; qi < q.length; qi++) {
      const idx = t.indexOf(q[qi], ti);
      if (idx < 0) return null;
      if (idx === prev + 1) score += 15;                      // consecutive run
      else if (idx === 0 || /[^a-z0-9]/.test(t[idx - 1])) score += 10; // word start
      else score += 1;
      score -= (idx - ti) * 0.5;                              // penalise gaps
      positions.push(idx);
      prev = idx;
      ti = idx + 1;
    }
    return { score: score * 2 - t.length * 0.1, positions };
  }

  // Match one search term against a job: fuzzy on name and job id, plain substring elsewhere.
  function matchTerm(term, j) {
    const name = fuzzyMatch(term, j.name), id = fuzzyMatch(term, j.job_id);
    const other = `${j.cluster} ${j.state} ${j.node_list} ${j.reason} ${j.partition} ${j.account} ${j.work_dir} ${j.exit_summary} ${stateLabel(j.state)}`
      .toLowerCase().includes(term.toLowerCase());
    if (!name && !id && !other) return null;
    return {
      score: Math.max(name ? name.score : -Infinity, id ? id.score : -Infinity, other ? 50 : -Infinity),
      namePos: name ? name.positions : [],
      idPos: id ? id.positions : [],
    };
  }

  function highlight(text, positions) {
    if (!positions || !positions.length) return text;
    const set = new Set(positions);
    const frag = document.createDocumentFragment();
    let run = "", inMark = false;
    const flush = () => { if (run) frag.append(inMark ? el("mark", {}, run) : run); run = ""; };
    for (let i = 0; i < text.length; i++) {
      const m = set.has(i);
      if (m !== inMark) { flush(); inMark = m; }
      run += text[i];
    }
    flush();
    return frag;
  }

  // ---------- data ----------
  // The page does as little as possible: it asks the server every POLL_MS with the
  // ETag it already has (a 304 costs nothing and triggers no work), re-renders only
  // when a new snapshot arrives, and stops polling altogether while the tab is hidden.
  let stateEtag = null;
  let stateTimer = null;
  const CLIENT_ID = (() => { try { return sessionStorage.getItem("omniqueue.client") || (sessionStorage.setItem("omniqueue.client", "d-" + Math.random().toString(36).slice(2)), sessionStorage.getItem("omniqueue.client")); } catch { return "d-" + Math.random().toString(36).slice(2); } })();
  async function fetchState() {
    try {
      // the client headers tell the server a dashboard is watching (it then polls at refresh_seconds)
      const headers = { "X-OmniQueue-Client": CLIENT_ID, "X-OmniQueue-Interval": "0" };
      if (stateEtag) headers["If-None-Match"] = stateEtag;
      const res = await fetch("/api/state", { cache: "no-store", headers });
      if (res.status === 304) { if (state.error) { state.error = null; renderHeader(); } return; }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      stateEtag = res.headers.get("ETag");
      const snap = await res.json();
      const changed = !state.snapshot || snap.last_refresh !== state.snapshot.last_refresh
        || snap.refreshing !== state.snapshot.refreshing || state.error;
      state.snapshot = snap;
      state.error = null;
      if (changed) render(); else renderHeader();
    } catch (err) {
      state.error = `dashboard cannot reach the OmniQueue server (${err.message})`;
      render();
    }
  }
  async function requestRefresh() {
    try { await post("/api/refresh"); } catch { /* shown on next poll */ }
    $("#refresh-state").textContent = "refreshing…";
    $("#refresh-state").classList.add("spin");
    setTimeout(fetchState, 800);
  }
  async function forgetJob(key) {
    await post(`/api/forget/${encodeURIComponent(key)}`);
    closeDrawer();
    setTimeout(fetchState, 500);
  }

  function clusterColor(name) {
    const c = state.snapshot?.clusters.find((x) => x.name === name);
    return c?.color || autoColor(name);
  }
  // fixed order: teal, coral, mustard, arctic, spruce, blush, sage, peacock
  const AUTO = ["#4f8f8a", "#e2856c", "#b39a4b", "#a9cbc8", "#3a615b", "#f3c6b6", "#c8b47c", "#073a34"];
  const autoIndex = new Map();
  function autoColor(name) {
    if (!autoIndex.has(name)) autoIndex.set(name, autoIndex.size);
    return AUTO[autoIndex.get(name) % AUTO.length];
  }

  function visibleJobs() {
    const snap = state.snapshot;
    if (!snap) return [];
    const q = state.search.trim().toLowerCase();
    state.matches = new Map();
    const cutoff = state.windowHours ? Date.now() - state.windowHours * 3600e3 : 0;
    return snap.jobs.filter((j) => {
      if (state.hiddenClusters.has(j.cluster)) return false;
      if (state.tab !== "all" && j.category !== state.tab && !(state.tab === "problem" && j.category === "unknown")) return false;
      if (cutoff && j.terminal) {
        const end = parseSlurmTime(j.end_time) || new Date(j.last_seen * 1000);
        if (end && end.getTime() < cutoff) return false;
      }
      if (q) {
        let score = 0; const namePos = [], idPos = [];
        for (const term of q.split(/\s+/)) {
          const m = matchTerm(term, j);
          if (!m) return false;
          score += m.score; namePos.push(...m.namePos); idPos.push(...m.idPos);
        }
        state.matches.set(j.key, { score, namePos, idPos });
      }
      return true;
    });
  }

  function sortJobs(jobs) {
    const { key, desc } = state.sort;
    const dir = desc ? -1 : 1;
    const val = (j) => {
      const v = j[key];
      if (key.endsWith("_time")) return parseSlurmTime(v)?.getTime() ?? -Infinity;
      if (key === "job_id") return parseFloat(String(v).replace(/[^\d.]/g, "")) || 0;
      if (typeof v === "number") return v;
      return v === null || v === undefined ? "" : String(v).toLowerCase();
    };
    const searching = state.search.trim() !== "";
    return jobs.sort((a, b) => {
      if (searching) {
        const sa = state.matches.get(a.key)?.score ?? 0, sb = state.matches.get(b.key)?.score ?? 0;
        if (sa !== sb) return sb - sa;
      }
      const va = val(a), vb = val(b);
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return a.job_id < b.job_id ? -1 : 1;
    });
  }

  // ---------- render ----------
  function render() {
    renderHeader();
    renderClusters();
    renderTabs();
    renderView();
    if (state.selected) renderDrawer();
  }

  function renderView() {
    const load = state.view === "load", predict = state.view === "predict", project = state.view === "project";
    $("#load").hidden = !load;
    $("#predict").hidden = !predict;
    $("#project").hidden = !project;
    $("#jobs-view").hidden = load || predict || project;
    $("#view-toggle").classList.toggle("active", load);
    $("#predict-toggle").classList.toggle("active", predict);
    $("#predict-toggle").hidden = !state.experimental;
    for (const b of $$("#tabs button[data-tab]")) b.disabled = load || predict || project;
    if (load) renderLoad(); else if (predict) renderPredict(); else if (project) renderProjectView(); else renderTable();
  }

  // `l`: enter the load view and fetch; `l` again (or the button): fetch again. `q`: back to the queue.
  function enterLoadView() {
    state.view = "load";
    closeDrawer();
    renderView();
    refreshLoad();
  }
  function leaveLoadView() {
    if (state.view === "jobs") return;
    state.view = "jobs";
    stopLoadPolling();
    renderView();
  }
  async function refreshLoad() {
    try { await post("/api/load/refresh"); } catch { /* shown by the status line */ }
    state.load = { ...(state.load || {}), fetching: true };
    renderLoadStatus();
    startLoadPolling();
  }
  function startLoadPolling() {
    stopLoadPolling();
    state.loadTimer = setInterval(fetchLoad, 1000);
    fetchLoad();
  }
  function stopLoadPolling() {
    if (state.loadTimer) clearInterval(state.loadTimer);
    state.loadTimer = null;
  }
  let loadEtag = null;
  async function fetchLoad() {
    try {
      const res = await fetch("/api/load", { cache: "no-store", headers: loadEtag ? { "If-None-Match": loadEtag } : {} });
      if (res.status === 304) return;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      loadEtag = res.headers.get("ETag");
      state.load = await res.json();
    } catch (err) {
      state.load = { ...(state.load || {}), fetching: false, error: err.message };
    }
    if (!state.load.fetching) stopLoadPolling();
    if (state.view === "load") renderLoad();
  }
  function renderLoadStatus() {
    const l = state.load;
    const st = $("#load-status");
    if (!l) { st.textContent = ""; return; }
    st.classList.toggle("spin", !!l.fetching);
    st.textContent = l.fetching ? "fetching cluster load…"
      : l.error ? `cannot reach the OmniQueue server (${l.error})`
      : l.fetched_at ? `cluster load as of ${clock(l.fetched_at)} · fetched on demand only, not on the regular poll`
      : "no load fetched yet";
  }

  // ---------- cluster load view ----------
  const fmtInt = (n) => (n === null || n === undefined) ? "–" : Number(n).toLocaleString("en").replace(/,/g, " ");
  function fmtLimit(s) {
    if (!s) return "–";
    if (s % 86400 === 0) return `${s / 86400} d`;
    if (s % 3600 === 0) return `${s / 3600} h`;
    return fmtDuration(s);
  }

  function renderLoad() {
    renderLoadStatus();
    const root = $("#load-clusters");
    root.replaceChildren();
    const l = state.load;
    if (!l || !l.clusters) return;
    for (const c of l.clusters) {
      if (state.hiddenClusters.has(c.name)) continue;
      const box = el("section", { class: "load-cluster", style: `--card-color:${clusterColor(c.name)}` });
      const head = el("div", { class: "load-head" }, logoEl(c), el("b", {}, c.name));
      const sum = c.summary;
      if (sum && sum.utilisation !== null && sum.utilisation !== undefined) {
        const pct = Math.round(sum.utilisation * 100);
        head.append(
          el("span", { class: "gauge", title: `${fmtInt(sum.cores_allocated ?? sum.cpus_allocated)} of ${fmtInt(sum.cores_total ?? sum.cpus_total)} cores allocated` },
            el("span", { class: `gauge-bar ${pct >= 90 ? "hot" : ""}` }, el("i", { style: `width:${pct}%` })), `${pct}% cores busy`),
          el("span", { class: "muted" }, `${fmtInt(sum.nodes_idle)} idle of ${fmtInt(sum.nodes_total)} nodes · ${fmtInt(sum.jobs_running)} running · ${fmtInt(sum.jobs_pending)} queued (all users)`),
          el("span", { class: "legend" }, ...["idle", "mixed", "allocated", "unavailable"].map((k) => el("span", { class: k }, k === "unavailable" ? "down/drained" : k))),
        );
      }
      if (sum && sum.threads_per_core > 1) head.append(el("span", { class: "load-filter", title: "sinfo reports CPUs as hardware threads here; the table shows physical cores" }, `Slurm counts ${sum.threads_per_core} threads per core here; shown as cores`));
      if (c.filter && c.filter.length) head.append(el("span", { class: "load-filter" }, `partitions: ${c.filter.join(", ")}`));
      if (c.fetched_at) head.append(el("span", { class: "load-filter" }, `as of ${clock(c.fetched_at)}`));
      box.append(head);
      if (!c.partitions || !c.partitions.length) {
        box.append(el("div", { class: "load-empty" },
          c.error ? c.error : (l.fetching ? "fetching…" : "no partition data yet")));
        root.append(box);
        continue;
      }
      if (c.warning) box.append(el("div", { class: "load-empty" }, `⚠ ${c.warning}`));
      // two groups: CPU partitions and GPU partitions (those sinfo reports GPUs for)
      const cpuParts = c.partitions.filter((p) => !p.gpu), gpuParts = c.partitions.filter((p) => p.gpu);
      const header = (gpu) => el("tr", {},
        el("th", {}, "Partition"), el("th", {}, "Nodes"), el("th", { class: "num" }, "Free nodes"), el("th", { class: "num" }, "Total"),
        gpu ? el("th", { class: "num", title: "GPUs on fully idle nodes / all GPUs in the partition" }, "Free GPUs") : null,
        el("th", { class: "num", title: "physical cores: idle / total" }, "Free cores"), el("th", { class: "num" }, "Max time"),
        el("th", { class: "num", title: "running jobs, all users" }, "Running"),
        el("th", { class: "num", title: "pending jobs from all users; array tasks counted individually" }, "Queued jobs"),
        el("th", { class: "num", title: "nodes those queued jobs need in total: the larger of the requested node count and requested CPUs / CPUs per node" }, "Nodes needed"));
      const group = (parts, gpu) => el("table", { class: `parts ${gpu ? "gpu" : "cpu"}` },
        el("thead", {}, gpuParts.length ? el("tr", { class: "group-row" }, el("th", { colspan: gpu ? 10 : 9 }, gpu ? `GPU partitions · ${parts.map((p) => `${p.partition}: ${p.gpus_per_node} GPUs/node`).join(", ")}` : "CPU partitions")) : null, header(gpu)),
        el("tbody", {}, ...parts.map((p) => partitionRow(p, gpu))));
      const wrap = el("div", { class: "parts-wrap" });
      if (cpuParts.length) wrap.append(group(cpuParts, false));
      if (gpuParts.length) wrap.append(group(gpuParts, true));
      box.append(wrap);
      root.append(box);
    }
    if (!root.children.length) root.append(el("div", { class: "load-empty" }, "no clusters to show"));
  }

  function partitionRow(p, gpu = false) {
    const n = p.nodes, total = n.total || 1;
    const seg = (k) => el("i", { class: k, style: `width:${(n[k] / total * 100).toFixed(1)}%`, title: `${n[k]} ${k}` });
    const wanted = p.pending_nodes;
    const pressure = n.idle > 0 ? "" : (wanted > 0 ? "high" : "");
    return el("tr", {},
      el("td", {}, p.partition,
        p.default ? el("span", { class: "default-tag" }, "default") : null,
        p.avail && p.avail !== "up" ? el("span", { class: "down-tag" }, p.avail) : null),
      el("td", {}, el("span", { class: "stack", title: `${n.idle} idle · ${n.mixed} mixed · ${n.allocated} allocated · ${n.unavailable} down/drained` },
        seg("idle"), seg("mixed"), seg("allocated"), seg("unavailable"))),
      el("td", { class: `num free ${n.idle ? "" : "none"}` }, fmtInt(n.idle)),
      el("td", { class: "num" }, fmtInt(n.total)),
      gpu ? el("td", { class: `num free ${p.gpus?.idle ? "" : "none"}`, title: "GPUs on fully idle nodes; GPUs free on partly used nodes are not visible to sinfo" }, `${fmtInt(p.gpus?.idle ?? 0)} / ${fmtInt(p.gpus?.total ?? 0)}`) : null,
      el("td", { class: "num", title: p.threads_per_core > 1 ? `${fmtInt(p.cpus.idle)} / ${fmtInt(p.cpus.total)} Slurm CPUs (${p.threads_per_core} threads per core)` : "" },
        `${fmtInt((p.cores || p.cpus).idle)} / ${fmtInt((p.cores || p.cpus).total)}`),
      el("td", { class: "num" }, fmtLimit(p.time_limit_s)),
      el("td", { class: "num" }, fmtInt(p.jobs.running)),
      el("td", { class: `num pressure ${pressure}` }, fmtInt(p.jobs.pending)),
      el("td", { class: `num pressure ${pressure}`, title: "nodes the queued jobs need in total (estimate)" }, fmtInt(wanted)),
    );
  }

  function renderHeader() {
    const snap = state.snapshot;
    const line = $("#summary-line");
    const rs = $("#refresh-state");
    if (!snap) { line.textContent = state.error || "connecting…"; return; }
    const counts = { running: 0, pending: 0, ok: 0, problem: 0 };
    for (const j of snap.jobs) if (counts[j.category] !== undefined) counts[j.category]++;
    const okClusters = snap.clusters.filter((c) => c.ok).length;
    line.textContent = snap.offline
      ? `no cluster reachable, are you offline? retrying at ${clock(snap.next_refresh)} · showing last known jobs`
      : `${okClusters}/${snap.clusters.length} clusters · ${counts.running} running · ${counts.pending} pending · ${counts.problem} failed`;
    line.classList.toggle("offline", !!snap.offline);
    rs.classList.toggle("spin", !!snap.refreshing);
    rs.textContent = snap.refreshing ? "refreshing…" : `polled ${clock(snap.last_refresh)} · every ${Math.round(snap.effective_refresh || snap.refresh_seconds)}s`;
    const windowLabel = { 24: "the last 24 h", 72: "the last 3 days", 168: "the last 7 days", 0: `the last ${snap.history_days} days of local history` }[state.windowHours]
      || `the last ${state.windowHours} h`;
    $("#footer-note").textContent = state.error
      ? state.error
      : `showing finished jobs from ${windowLabel} · clusters are asked for the last ${snap.lookback_hours} h, older jobs come from the local history · times as the cluster reports them`;
  }

  function renderClusters() {
    const root = $("#clusters");
    root.replaceChildren();
    const snap = state.snapshot;
    if (!snap) return;
    for (const c of snap.clusters) {
      const hidden = state.hiddenClusters.has(c.name);
      const counts = c.counts || {};
      const card = el("div", {
        class: `card ${hidden ? "off" : ""} ${c.ok || c.error_kind === "login" ? "" : "err"}`,
        style: `--card-color:${clusterColor(c.name)}`,
        title: hidden ? "click to show this cluster's jobs" : "click to hide this cluster's jobs",
        onclick: () => { hidden ? state.hiddenClusters.delete(c.name) : state.hiddenClusters.add(c.name); savePrefs(); render(); },
      },
        el("div", { class: "card-head" }, logoEl(c), el("b", {}, c.name), el("small", {}, c.host)),
        c.ok
          ? el("div", { class: "card-counts" },
              ...[["running", "running"], ["pending", "pending"], ["problem", "failed"], ["ok", "done"]].map(([k, label]) =>
                el("span", { class: `pill ${k}`, title: label }, el("b", {}, counts[k] || 0), label)))
          : el("div", { class: `card-error ${c.error_kind || ""}` }, el("span", { class: "warn-icon" }, c.error_kind === "login" ? "⏻" : "⚠"),
              el("span", {}, el("span", {}, c.error || "not reached yet"), el("small", { class: "hint" }, errorHint(c)))),
        el("div", { class: "card-foot" },
          el("span", {}, c.ok ? `polled ${clock(c.last_success)}` : c.last_success ? `last ok ${clock(c.last_success)}` : "never reached"),
          connectionEl(c),
        ),
        c.ok && c.warning ? el("div", { class: "card-warn" }, `⚠ ${c.warning}`) : null,
      );
      root.append(card);
    }
  }

  function connectionEl(c) {
    if (c.connected === null || c.connected === undefined) {
      return el("span", {}, c.ok && c.poll_seconds != null ? `${c.poll_seconds.toFixed(1)}s` : "");
    }
    return el("span", { class: `conn ${c.connected ? "on" : "off"}`,
      title: c.connected ? "persistent ssh connection is open" : "no ssh connection open; the next poll reconnects if keys allow, otherwise run omniqueue login" },
      c.connected ? "ssh connected" : "ssh not connected");
  }

  function errorHint(c) {
    const snap = state.snapshot;
    const retry = snap?.next_refresh ? ` · retry at ${clock(snap.next_refresh)}` : "";
    switch (c.error_kind) {
      case "login": return `run  omniqueue login ${c.name}  to start polling`;
      case "auth": return `login needed: run  omniqueue login ${c.name}${retry}`;
      case "network": return `network problem, are you online?${retry}`;
      case "timeout": return `no answer, connection dropped${retry}`;
      default: return c.failures > 1 ? `${c.failures} failed polls${retry}` : retry.slice(3);
    }
  }

  function logoEl(c) {
    if (c.logo) {
      const img = el("img", { class: "card-logo", src: c.logo, alt: "" });
      img.addEventListener("error", () => img.replaceWith(initialsEl(c)), { once: true });
      return img;
    }
    return initialsEl(c);
  }
  function initialsEl(c) {
    const parts = c.name.split(/[^a-z0-9]+/i).filter(Boolean);
    const text = (parts.length > 1 ? parts[0][0] + parts[1][0] : c.name.slice(0, 2)).toUpperCase();
    return el("span", { class: "card-logo initials", "aria-hidden": "true" }, text);
  }

  function renderTabs() {
    const snap = state.snapshot;
    const counts = { all: 0, running: 0, pending: 0, ok: 0, problem: 0 };
    if (snap) {
      const cutoff = state.windowHours ? Date.now() - state.windowHours * 3600e3 : 0;
      for (const j of snap.jobs) {
        if (state.hiddenClusters.has(j.cluster)) continue;
        if (cutoff && j.terminal) {
          const end = parseSlurmTime(j.end_time) || new Date(j.last_seen * 1000);
          if (end && end.getTime() < cutoff) continue;
        }
        counts.all++;
        const k = j.category === "unknown" ? "problem" : j.category;
        if (counts[k] !== undefined) counts[k]++;
      }
    }
    for (const b of $$("#tabs button[data-tab]")) {
      b.classList.toggle("active", b.dataset.tab === state.tab);
      $(".count", b).textContent = counts[b.dataset.tab] ?? 0;
    }
  }

  function renderTable() {
    const tbody = $("#jobs tbody");
    const visible = new Set(visibleJobs().map((j) => j.key));
    // group array tasks; an array shows when any of its tasks passes the filters, with counts over all its tasks
    const items = OQ.groupArrays(state.snapshot ? state.snapshot.jobs : [])
      .filter((it) => it.kind === "job" ? visible.has(it.job.key) : it.tasks.some((t) => visible.has(t.key)));
    sortItems(items);
    $("#empty").hidden = items.length > 0;
    for (const th of $$("#jobs th")) {
      th.classList.toggle("sorted", th.dataset.sort === state.sort.key);
      th.classList.toggle("desc", th.dataset.sort === state.sort.key && state.sort.desc);
    }
    const rows = [];
    for (const it of items) {
      if (it.kind === "job") { rows.push(jobRow(it.job)); continue; }
      rows.push(arrayRow(it));
      if (state.expanded.has(it.key)) {
        for (const t of sortJobs(it.tasks.filter((t) => visible.has(t.key)))) rows.push(jobRow(t, true));
      }
    }
    tbody.replaceChildren(...rows);
  }

  function sortItems(items) {
    const { key, desc } = state.sort;
    const dir = desc ? -1 : 1;
    const val = (it) => {
      const o = it.kind === "job" ? it.job : it;
      if (key === "job_id") return parseFloat(String(it.kind === "job" ? o.job_id : it.base).replace(/[^\d.]/g, "")) || 0;
      if (key === "elapsed_s") return it.kind === "job" ? (o.elapsed_s ?? -1) : it.elapsed_max;
      const v = o[key];
      if (key.endsWith("_time")) return parseSlurmTime(v)?.getTime() ?? -Infinity;
      if (typeof v === "number") return v;
      return v == null ? "" : String(v).toLowerCase();
    };
    const searching = state.search.trim() !== "";
    const score = (it) => it.kind === "job" ? (state.matches.get(it.job.key)?.score ?? 0)
      : Math.max(...it.tasks.map((t) => state.matches.get(t.key)?.score ?? 0));
    items.sort((a, b) => {
      if (searching) { const sa = score(a), sb = score(b); if (sa !== sb) return sb - sa; }
      const va = val(a), vb = val(b);
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return a.key < b.key ? -1 : 1;
    });
  }

  function jobRow(j, isTask = false) {
    const note = j.category === "pending" ? j.reason : (j.exit_summary || (j.category === "unknown" ? j.reason : ""));
    const pendingEstimate = j.category === "pending" && j.start_time;
    const idText = isTask ? `↳ ${j.job_id.split("_")[1] ?? j.job_id}` : j.job_id;
    return el("tr", { class: `${state.selected === j.key ? "selected" : ""} ${isTask ? "task" : ""}`, "data-key": j.key, onclick: () => openDrawer(j.key) },
      el("td", {}, isTask ? "" : el("span", { class: "cl", style: `--card-color:${clusterColor(j.cluster)}` }, j.cluster)),
      el("td", { class: "mono", title: j.job_id }, isTask ? idText : highlight(j.job_id, state.matches.get(j.key)?.idPos)),
      el("td", { class: "name", title: j.name }, isTask ? el("span", { class: "muted" }, `task ${j.job_id.split("_")[1] ?? ""}`) : highlight(j.name, state.matches.get(j.key)?.namePos)),
      el("td", {}, el("span", { class: `state ${j.category}` }, stateLabel(j.state))),
      el("td", { class: "num mono" }, elapsedCell(j)),
      el("td", { class: "num mono" }, fmtDuration(j.time_limit_s)),
      el("td", { class: "num" }, j.nodes || "–"),
      el("td", { class: "mono", title: j.submit_time }, fmtTime(j.submit_time)),
      pendingEstimate
        ? el("td", { class: "mono estimate", title: `Slurm's estimated start (backfill): ${j.start_time}` }, `~${fmtTime(j.start_time)}`)
        : el("td", { class: "mono", title: j.start_time }, fmtTime(j.start_time)),
      el("td", { class: "mono", title: j.end_time }, fmtTime(j.end_time)),
      el("td", { class: `note ${j.category === "problem" || j.category === "unknown" ? "problem" : ""}`, title: note }, note || ""),
    );
  }

  function toggleAllArrays() {
    if (!state.snapshot) return;
    const keys = OQ.groupArrays(state.snapshot.jobs).filter((it) => it.kind === "array").map((it) => it.key);
    if (keys.some((k) => state.expanded.has(k))) state.expanded.clear(); else for (const k of keys) state.expanded.add(k);
    renderTable();
  }

  function arrayRow(g) {
    const open = state.expanded.has(g.key);
    const frac = g.total ? g.done / g.total : 0;
    const toggle = (e) => { e.stopPropagation(); if (open) state.expanded.delete(g.key); else state.expanded.add(g.key); renderTable(); };
    const c = g.counts;
    return el("tr", { class: `array ${open ? "open" : ""}`, "data-key": g.key, onclick: toggle, title: open ? "click to collapse the tasks" : "click to show the tasks" },
      el("td", {}, el("span", { class: "cl", style: `--card-color:${clusterColor(g.cluster)}` }, g.cluster)),
      el("td", { class: "mono" }, el("span", { class: "caret" }, open ? "▾" : "▸"), `${g.base}_[${g.total}]`),
      el("td", { class: "name", title: `${g.name} · job array with ${g.total} tasks` }, g.name, el("span", { class: "array-tag" }, `array · ${g.total}`)),
      el("td", { class: "array-states" },
        c.running ? el("span", { class: "state running" }, `${c.running} running`) : null,
        c.pending ? el("span", { class: "state pending" }, `${c.pending} waiting`) : null,
        c.ok ? el("span", { class: "state ok" }, `${c.ok} done`) : null,
        c.problem ? el("span", { class: "state problem" }, `${c.problem} failed`) : null),
      el("td", { class: "num mono" },
        el("span", { class: "bar done-bar", title: `${g.done} of ${g.total} tasks finished` },
          el("i", { style: `width:${(frac * 100).toFixed(1)}%` }), el("span", {}, `${g.done}/${g.total}`))),
      el("td", { class: "num mono" }, fmtDuration(g.time_limit_s)),
      el("td", { class: "num" }, g.nodes || "–"),
      el("td", { class: "mono", title: g.submit_time }, fmtTime(g.submit_time)),
      el("td", { class: "mono", title: g.start_time }, fmtTime(g.start_time)),
      el("td", { class: "mono", title: g.end_time }, fmtTime(g.end_time)),
      el("td", { class: `note ${c.problem ? "problem" : ""}` }, c.problem ? g.exit_summary : (c.pending && !c.running ? `${c.pending} of ${g.total} waiting to start` : "")),
    );
  }

  function elapsedCell(j) {
    if (j.category !== "running" || !j.time_limit_s) return fmtDuration(j.elapsed_s);
    const frac = Math.min(1, (j.elapsed_s || 0) / j.time_limit_s);
    return el("span", {},
      el("span", { class: "bar " + (frac > 0.9 ? "hot" : ""), title: `${Math.round(frac * 100)}% of time limit used` },
        el("i", { style: `width:${(frac * 100).toFixed(1)}%` }),
        el("span", {}, fmtDuration(j.elapsed_s))));
  }

  // ---------- drawer ----------
  function openDrawer(key) { state.selected = key; renderDrawer(); renderTable(); }
  function closeDrawer() { state.selected = null; $("#drawer").hidden = true; renderTable(); }
  function renderDrawer() {
    const j = state.snapshot?.jobs.find((x) => x.key === state.selected);
    const drawer = $("#drawer");
    if (!j) { drawer.hidden = true; return; }
    drawer.hidden = false;
    $("#drawer-title").textContent = `${j.cluster} · ${j.job_id} · ${j.name}`;
    const fields = [
      ["state", el("span", { class: `state ${j.category}` }, stateLabel(j.state))],
      ["note", j.exit_summary || j.reason || "–"],
      ["exit code", j.exit_code || "–"],
      ["elapsed", fmtDuration(j.elapsed_s)],
      ["time limit", fmtDuration(j.time_limit_s)],
      ["submitted", j.submit_time || "–"],
      [j.category === "pending" ? "est. start" : "started", j.start_time || "–"],
      ["ended", j.end_time || "–"],
      ["queue wait", waitTime(j)],
      ["nodes", `${j.nodes || "–"}${j.node_list ? "  " + j.node_list : ""}`],
      ["cpus", j.cpus || "–"],
      ["partition", j.partition || "–"],
      ["account", j.account || "–"],
      ["user", j.user || "–"],
      ["work dir", j.work_dir || "–"],
      ["source", `${j.source} · seen ${ago(j.last_seen)}`],
    ];
    $("#drawer-body").replaceChildren(...fields.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, v)]));
    $("#forget").onclick = () => { if (confirm(`Remove ${j.job_id} on ${j.cluster} from the local history?`)) forgetJob(j.key); };
    $("#forget").hidden = !j.terminal && j.state !== "VANISHED";
  }
  function waitTime(j) {
    const s = parseSlurmTime(j.submit_time), st = parseSlurmTime(j.start_time);
    if (!s || !st) return "–";
    return fmtDuration((st - s) / 1000);
  }

  // ---------- projects (slow background poll, own card row) ----------
  // The server polls each cluster's projects every few hours on its own; the page only
  // re-reads the result (a 304 when nothing changed) and never triggers cluster work itself.
  let projEtag = null;
  const PROJ_POLL_MS = 60000;
  async function fetchProjects() {
    try {
      const res = await fetch("/api/projects", { cache: "no-store", headers: projEtag ? { "If-None-Match": projEtag } : {} });
      if (res.status === 304) return;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      projEtag = res.headers.get("ETag");
      state.projects = await res.json();
      renderProjects();
      if (state.view === "project") renderProjectView();
    } catch { /* the cluster cards already show that the server is unreachable */ }
    if (state.projects?.projects?.some((p) => p.fetching || p.queue_fetching || p.backfill_pending)) setTimeout(fetchProjects, p_backfill_ms(state.projects));
  }
  const p_backfill_ms = (pr) => (pr.projects.some((p) => p.fetching || p.queue_fetching) ? 2000 : 15000); // chunks arrive a minute apart
  const pr_history_days = () => state.projects?.history_days || 90;
  async function refreshQueue(cluster) {
    try {
      await fetch("/api/projects/queue/refresh", { method: "POST", headers: { "X-OmniQueue-Token": TOKEN, "Content-Type": "application/json" }, body: JSON.stringify(cluster ? { cluster } : {}) });
    } catch { /* ignore */ }
    setTimeout(fetchProjects, 600);
  }
  async function refreshProjects() {
    try { await post("/api/projects/refresh"); } catch { /* ignore */ }
    $("#proj-status").textContent = "polling the projects…";
    setTimeout(fetchProjects, 700);
  }
  // one colour per person, shared across cards. Neighbours differ in hue *and* lightness so
  // adjacent bar segments stay apart; no red next to green anywhere.
  const USER_COLORS = ["#4f8f8a", "#e2856c", "#e0b84a", "#8c6d9e", "#a9cbc8", "#c9674e", "#6f8fb3", "#f3c6b6",
                       "#3a615b", "#d19a3d", "#b08bbf", "#7fbdb7", "#9c5a44", "#cfc28a", "#4a6fa5", "#e8a48f"];
  const userIndex = new Map();
  function userColor(u) {
    if (!userIndex.has(u)) userIndex.set(u, userIndex.size);
    return USER_COLORS[userIndex.get(u) % USER_COLORS.length];
  }
  const fmtCoreH = (x) => (x >= 10000 ? `${(x / 1000).toFixed(0)}k` : x >= 1000 ? `${(x / 1000).toFixed(1)}k` : `${Math.round(x)}`);
  function fmtEvery(s) { return s % 3600 === 0 ? `${s / 3600} h` : `${Math.round(s / 60)} min`; }
  const plural = (n, word) => `${fmtInt(n)} ${word}${n === 1 ? "" : "s"}`;

  function stackBar(parts, total, title) {
    // parts: [[user, value]] sorted; renders a stacked bar with per-user tooltips
    const bar = el("div", { class: "pstack", title });
    if (!total) { bar.classList.add("nodata"); return bar; }
    for (const [u, v] of parts) {
      if (v <= 0) continue;
      bar.append(el("i", { style: `width:${(v / total * 100).toFixed(2)}%;background:${userColor(u)}`, title: `${u}: ${Math.round(v).toLocaleString("en").replace(/,/g, " ")}` }));
    }
    return bar;
  }

  const fmtGpuH = (x) => `${fmtCoreH(x)} GPU-h`;
  function usageRow(label, valueText, title, parts, total, barTitle) {
    return el("div", { class: "prow" }, el("span", { class: "plabel" }, label),
      el("span", { class: "pval", title }, valueText), stackBar(parts, total, barTitle));
  }

  function projectCard(p) {
    const color = p.color || clusterColor(p.cluster);
    const card = el("article", { class: "card pcard", style: `--card-color:${color}`, title: "click for the project's running and waiting jobs",
      onclick: (e) => { if (!e.target.closest("a, button")) enterProjectView(p.cluster, p.project); } });
    const updated = p.updated ? `updated ${clock(p.updated).slice(0, 5)}` : "no data yet";
    // stored history may reach further back than the re-fetched part (records older than the marker are still shown)
    const storedDays = p.oldest ? Math.min(pr_history_days(), (Date.now() / 1000 - p.oldest) / 86400) : 0;
    const cover = p.coverage_days == null ? ""
      : !p.backfill_pending ? ` · ${Math.round(p.coverage_days)} d`
      : storedDays > p.coverage_days + 1 ? ` · updating history: ${Math.round(p.coverage_days)} of ${Math.round(storedDays)} d`
      : ` · loading history: ${Math.round(p.coverage_days)} d so far`;
    card.append(el("div", { class: "pcard-head" },
      el("span", { class: "w-cpill", style: `background:${color};color:#fff` }, p.cluster),
      el("b", {}, p.project),
      el("small", { class: "muted", title: `queue as of ${p.updated ? clock(p.updated) : "–"}; the full poll (accounting, fairshare, load) runs every ${fmtEvery(p.refresh_seconds)} in the background, next ${p.next_poll ? clock(p.next_poll).slice(0, 5) : "–"}` },
        `${updated} · every ${fmtEvery(p.refresh_seconds)}${cover}`),
      el("button", { class: `qrefresh ${p.queue_fetching ? "spin" : ""}`, title: "re-read this cluster's project queue now (squeue only, no accounting)",
        onclick: (e) => { e.stopPropagation(); refreshQueue(p.cluster); } }, "↻")));
    if (p.error && !p.updated) {
      const kind = p.error_kind || "";
      card.append(el("div", { class: `card-error ${kind}` }, el("span", { class: "warn-icon" }, kind === "login" ? "○" : "⚠"),
        el("span", {}, kind === "login" ? "not logged in: run omniqueue login to start collecting" : p.error)));
      return card;
    }
    const u30 = p.usage["30"], u7 = p.usage["7"];
    const users = p.users.slice();
    const rc = p.running.cpu, rg = p.running.gpu, qc = p.pending.cpu, qg = p.pending.gpu;
    const rows = el("div", { class: "prows" });
    // CPU side: cores now, core-hours over 30 days
    rows.append(usageRow(p.has_gpu ? "cpu now" : "running now",
      el("span", {}, `${plural(rc.jobs, "job")} · ${fmtInt(Math.round(rc.cores))} cores`, qc.jobs ? el("span", { class: "muted" }, ` · ${fmtInt(qc.jobs)} waiting`) : null),
      `${rc.jobs} running jobs on ${rc.nodes} nodes, ${qc.jobs} waiting (${Math.round(qc.cores)} cores asked for)` + (p.has_gpu ? "; CPU jobs only, GPU jobs are counted in the GPU rows" : ""),
      users.map((u) => [u, rc.users[u]?.cores || 0]).filter(([, v]) => v > 0), rc.cores, "cores in use right now, by user"));
    rows.append(usageRow(p.has_gpu ? "cpu 30 d" : "last 30 d",
      el("span", {}, `${fmtCoreH(u30.cpu.core_h)} core-h · ${plural(u30.cpu.jobs, "job")}`, el("span", { class: "muted" }, ` · 7 d ${fmtCoreH(u7.cpu.core_h)}`)),
      `${Math.round(u30.cpu.core_h).toLocaleString("en")} core-hours in ${u30.cpu.jobs} jobs over 30 days; ${Math.round(u7.cpu.core_h).toLocaleString("en")} in the last 7 days` + (p.has_gpu ? "; CPU jobs only, GPU jobs are counted in the GPU rows" : ""),
      users.map((u) => [u, u30.cpu.users[u]?.core_h || 0]).filter(([, v]) => v > 0), u30.cpu.core_h, "core-hours in the last 30 days, by user"));
    if (p.has_gpu) {
      // GPU side: GPUs now, GPU-hours over 30 days (jobs with GPUs or on a GPU partition)
      rows.append(usageRow("gpu now",
        el("span", {}, `${plural(rg.jobs, "job")} · ${fmtInt(Math.round(rg.gpus))} GPUs`, qg.jobs ? el("span", { class: "muted" }, ` · ${fmtInt(qg.jobs)} waiting`) : null),
        `${rg.jobs} running GPU jobs on ${rg.nodes} nodes, ${qg.jobs} waiting (${Math.round(qg.gpus)} GPUs asked for)` + (p.gpu_partitions.length ? `; GPU partitions: ${p.gpu_partitions.join(", ")}` : "") + (p.gpu_factor !== 1 ? `; counted in Slurm GPU units (GPU-hours are billed x ${p.gpu_factor})` : ""),
        users.map((u) => [u, rg.users[u]?.gpus || 0]).filter(([, v]) => v > 0), rg.gpus, "GPUs in use right now, by user"));
      rows.append(usageRow("gpu 30 d",
        el("span", {}, `${fmtGpuH(u30.gpu.gpu_h)} · ${plural(u30.gpu.jobs, "job")}`, el("span", { class: "muted" }, ` · 7 d ${fmtCoreH(u7.gpu.gpu_h)}`)),
        `${Math.round(u30.gpu.gpu_h).toLocaleString("en")} GPU-hours in ${u30.gpu.jobs} GPU jobs over 30 days (${Math.round(u30.gpu.core_h).toLocaleString("en")} core-hours alongside); ${Math.round(u7.gpu.gpu_h).toLocaleString("en")} GPU-h in the last 7 days` + (p.gpu_factor !== 1 ? `; billed at ${p.gpu_factor} GPU-h per Slurm GPU unit and hour` : ""),
        users.map((u) => [u, u30.gpu.users[u]?.gpu_h || 0]).filter(([, v]) => v > 0), u30.gpu.gpu_h, "GPU-hours in the last 30 days, by user"));
    }
    card.append(rows);
    // daily chart(s), stacked by user: core-hours, and GPU-hours when the project has any
    const daily = p.daily || [];
    const dailyChart = (key, usersKey, unit) => {
      const max = Math.max(1, ...daily.map((d) => d[key]));
      const chart = el("div", { class: `pdaily ${key === "gpu_h" ? "gpu" : ""}`, title: `${unit} per day, last 30 days` });
      for (const d of daily) {
        const col = el("div", { class: "pday", title: `${d.date}: ${Math.round(d[key]).toLocaleString("en").replace(/,/g, " ")} ${unit}` });
        for (const u of users) {
          const v = d[usersKey][u] || 0;
          if (v > 0) col.append(el("i", { style: `height:${(v / max * 100).toFixed(1)}%;background:${userColor(u)}` }));
        }
        chart.append(col);
      }
      return chart;
    };
    card.append(dailyChart("core_h", "users", "core-h"));
    if (p.has_gpu && daily.some((d) => d.gpu_h > 0)) card.append(el("div", { class: "pchart-label" }, "GPU-h per day"), dailyChart("gpu_h", "gpu_users", "GPU-h"));
    // user legend: core-hours, and GPU-hours where they have any
    const legend = el("div", { class: `pusers ${users.length > 8 ? "many" : ""}` });
    for (const u of users) {
      const me = u === p.me;
      const fs = p.shares?.users?.[u]?.fairshare;
      const ch = u30.cpu.users[u]?.core_h || 0, gh = u30.gpu.users[u]?.gpu_h || 0;
      legend.append(el("span", { class: `puser ${me ? "me" : ""}`, title: `${u}: ${Math.round(ch).toLocaleString("en")} core-h and ${Math.round(gh).toLocaleString("en")} GPU-h in 30 d` + (fs != null ? `, fairshare ${fs.toFixed(2)}` : "") },
        el("i", { style: `background:${userColor(u)}` }), me ? `${u} (you)` : u, el("small", {}, fmtCoreH(ch)), gh > 0 ? el("small", { class: "gpu" }, `${fmtCoreH(gh)} GPU-h`) : null));
    }
    card.append(legend);
    const foot = el("div", { class: "card-foot" });
    const quotaEl = (q, unit) => {
      const frac = q.fraction ?? 0;
      return el("span", { class: "pquota", title: q.window === "30 d" ? `${unit} used in the last 30 days against the configured monthly quota` : `Slurm accounting limit from sshare (GrpTRESMins)` },
        el("span", { class: "gauge-bar " + (frac > 0.9 ? "hot" : "") }, el("i", { style: `width:${(frac * 100).toFixed(1)}%` })),
        `${fmtCoreH(q.used_h)} / ${fmtCoreH(q.limit_h)} ${unit} (${q.window})`);
    };
    if (p.quota) foot.append(quotaEl(p.quota, "core-h"));
    if (p.gpu_quota) foot.append(quotaEl(p.gpu_quota, "GPU-h"));
    if (p.shares?.fairshare != null) {
      const mine = p.shares.users?.[p.me]?.fairshare;
      foot.append(el("span", { title: "Slurm fairshare factor: 1 = front of the queue, 0 = back" },
        `fairshare ${p.shares.fairshare.toFixed(2)}${mine != null ? ` · yours ${mine.toFixed(2)}` : ""}`));
    }
    if (foot.childElementCount) card.append(foot);
    if (p.warning || (p.error && p.updated)) card.append(el("div", { class: "card-warn" }, p.error ? `last poll failed: ${p.error}` : p.warning));
    return card;
  }

  // ---------- project view: the project's own queue ----------
  function enterProjectView(cluster, project) {
    state.projectKey = `${cluster}/${project}`;
    state.view = "project";
    closeDrawer();
    renderView();
  }
  function renderProjectView() {
    const p = state.projects?.projects.find((x) => `${x.cluster}/${x.project}` === state.projectKey);
    const title = $("#project-title");
    if (!p) { title.textContent = "project not found"; $("#project-jobs tbody").replaceChildren(); return; }
    const color = p.color || clusterColor(p.cluster);
    const rc = p.running.cpu, rg = p.running.gpu, qc = p.pending.cpu, qg = p.pending.gpu;
    title.replaceChildren(el("span", { class: "w-cpill", style: `background:${color};color:#fff` }, p.cluster), el("b", {}, p.project),
      el("span", { class: "muted" }, ` · ${plural(rc.jobs + rg.jobs, "job")} running, ${fmtInt(qc.jobs + qg.jobs)} waiting`
        + (p.has_gpu ? ` · ${fmtInt(Math.round(rc.cores))} cores and ${fmtInt(Math.round(rg.gpus))} GPUs in use` + (p.gpu_factor !== 1 ? ` (Slurm units; billed x ${p.gpu_factor})` : "") : ` · ${fmtInt(Math.round(rc.cores))} cores in use`)));
    $("#project-note").textContent = p.queue_error ? `queue refresh failed: ${p.queue_error}`
      : p.queue_fetching ? "re-reading the queue…"
      : p.updated ? `every user's jobs in this project as of ${clock(p.updated).slice(0, 5)} (the full poll runs every ${fmtEvery(p.refresh_seconds)}; ↻ refresh queue re-reads only squeue); pending arrays count as one row`
      : "no data yet";
    $("#project-queue-refresh").onclick = () => refreshQueue(p.cluster);
    $("#project-queue-refresh").classList.toggle("spin", !!p.queue_fetching);
    const rows = (p.jobs_now || []).map((j) => {
      const me = j.user === p.me;
      const frac = j.category === "running" && j.time_limit_s ? Math.min(1, (j.elapsed_s || 0) / j.time_limit_s) : null;
      return el("tr", { class: me ? "me" : "" },
        el("td", {}, el("span", { class: "puser" }, el("i", { style: `background:${userColor(j.user)}` }), me ? `${j.user} (you)` : j.user)),
        el("td", { class: "mono" }, j.job_id),
        el("td", { class: "name", title: j.name }, j.name || ""),
        el("td", {}, el("span", { class: `state ${j.category}` }, j.category === "running" ? "running" : "pending"),
          j.tasks > 1 ? el("span", { class: "array-tag" }, `array · ${j.tasks}`) : null),
        el("td", { class: "num mono" }, frac === null ? fmtDuration(j.elapsed_s) : el("span", { class: "bar " + (frac > 0.9 ? "hot" : ""), title: `${Math.round(frac * 100)}% of time limit used` },
          el("i", { style: `width:${(frac * 100).toFixed(1)}%` }), el("span", {}, fmtDuration(j.elapsed_s)))),
        el("td", { class: "num mono" }, fmtDuration(j.time_limit_s)),
        el("td", { class: "num" }, j.nodes || "–"),
        el("td", { class: "num" }, fmtInt(Math.round(j.cores))),
        el("td", { class: `num ${j.kind === "gpu" ? "gpu" : "muted"}`, title: j.kind === "gpu" && p.gpu_factor !== 1 ? `${j.gpus} Slurm GPU units, billed as ${(j.gpus * p.gpu_factor).toFixed(1)} GPUs` : "" },
          j.kind === "gpu" ? fmtInt(Math.round(j.gpus)) : "–"),
        el("td", {}, j.partition || "–"));
    });
    $("#project-jobs tbody").replaceChildren(...rows);
    $("#project-jobs").hidden = !rows.length;
    $("#project-empty").hidden = !!rows.length;
  }
  $("#project-back").addEventListener("click", leaveLoadView);

  function renderProjects() {
    const pr = state.projects;
    const sec = $("#projects");
    if (!pr || !pr.enabled) { sec.hidden = true; return; }
    sec.hidden = false;
    // colour users by overall usage so the same person keeps their colour across cards
    const totals = new Map();
    for (const p of pr.projects) {
      for (const [u, v] of Object.entries(p.usage["30"].cpu.users)) totals.set(u, (totals.get(u) || 0) + v.core_h);
      for (const [u, v] of Object.entries(p.usage["30"].gpu.users)) totals.set(u, (totals.get(u) || 0) + v.gpu_h * 30 + v.core_h);
    }
    for (const [u] of [...totals].sort((a, b) => b[1] - a[1])) userColor(u);
    const fetching = pr.projects.some((p) => p.fetching);
    const filling = pr.projects.some((p) => p.backfill_pending && !p.error);
    $("#proj-status").textContent = fetching ? (pr.projects.some((p) => p.backfilling) ? "loading older history…" : "polling the projects…")
      : filling ? `history is being ${pr.projects.some((p) => p.backfill_pending && p.oldest && (Date.now() / 1000 - p.oldest) / 86400 > (p.coverage_days || 0) + 1) ? "updated" : "loaded"} in ${pr.backfill_days}-day chunks, a minute apart · polled every ${fmtEvery(pr.refresh_seconds)} · ${pr.history_days} d kept`
      : `who runs how much in your Slurm projects · polled in the background every ${fmtEvery(pr.refresh_seconds)} · ${pr.history_days} d kept`;
    $("#proj-status").classList.toggle("spin", fetching);
    const collapsed = state.projectsCollapsed;
    sec.classList.toggle("collapsed", collapsed);
    $("#proj-toggle").textContent = collapsed ? "▸" : "▾";
    $("#proj-toggle").title = collapsed ? "show the project cards (p)" : "collapse the project cards (p)";
    $("#proj-cards").hidden = collapsed;
    $("#proj-compact").hidden = !collapsed;
    if (collapsed) {
      // one pill per project: running jobs and the 30-day usage, enough to see life without the cards
      $("#proj-compact").replaceChildren(...pr.projects.map((p) => {
        const color = p.color || clusterColor(p.cluster);
        const u30 = p.usage["30"];
        const text = p.error && !p.updated ? "no data"
          : `${plural(p.running.cpu.jobs + p.running.gpu.jobs, "job")} running · ${fmtCoreH(u30.cpu.core_h)} core-h` + (p.has_gpu ? ` · ${fmtCoreH(u30.gpu.gpu_h)} GPU-h` : "") + " / 30 d";
        return el("span", { class: `pmini ${p.error && !p.updated ? "err" : ""}`, style: `--card-color:${color}`, title: `${p.cluster} · ${p.project}: ${p.error || text} (click to expand)`, onclick: toggleProjects },
          el("span", { class: "w-cpill", style: `background:${color};color:#fff` }, p.cluster), el("b", {}, p.project), el("span", { class: "muted" }, text));
      }));
    } else {
      $("#proj-cards").replaceChildren(...pr.projects.map(projectCard));
    }
    // the predictor's project list follows the configured projects
    const sel = $("#p-project");
    const cur = sel.value;
    sel.replaceChildren(el("option", { value: "" }, "any"), ...pr.projects.map((p) => el("option", { value: p.project }, `${p.project} (${p.cluster})`)));
    sel.value = cur;
  }
  function startProjectPolling() {
    if (state.projTimer) return;
    fetchProjects();
    state.projTimer = setInterval(fetchProjects, PROJ_POLL_MS);
  }
  function stopProjectPolling() {
    if (state.projTimer) clearInterval(state.projTimer);
    state.projTimer = null;
  }
  $("#proj-refresh").addEventListener("click", refreshProjects);
  function toggleProjects() {
    state.projectsCollapsed = !state.projectsCollapsed;
    savePrefs();
    renderProjects();
  }
  $("#proj-toggle").addEventListener("click", toggleProjects);
  $(".proj-title").addEventListener("click", toggleProjects);

  // ---------- experimental: where to submit? ----------
  // Unlocked by typing `experimental` in the search box (again to hide). The estimate is
  // computed server-side by omniqueue.predict from the samples the project poll stores.
  function setExperimental(on) {
    state.experimental = on;
    try { localStorage.setItem("omniqueue.experimental", on ? "1" : "0"); } catch { /* ignore */ }
    if (!on && state.view === "predict") state.view = "jobs";
    renderView();
    $("#summary-line").textContent = on ? "experimental features on: press x for “where to submit?”" : "experimental features off";
  }
  function enterPredictView() {
    if (!state.experimental) return;
    state.view = "predict";
    closeDrawer();
    renderView();
  }
  async function runPrediction(e) {
    e?.preventDefault();
    const body = { nodes: Number($("#p-nodes").value) || 1, hours: Number($("#p-hours").value) || 1 };
    if ($("#p-cores").value) body.cores = Number($("#p-cores").value);
    if (Number($("#p-gpus").value) > 0) body.gpus = Number($("#p-gpus").value);
    if ($("#p-project").value) body.projects = [$("#p-project").value];
    $("#predict-status").textContent = "estimating…";
    try {
      const res = await fetch("/api/experimental/predict", { method: "POST", headers: { "X-OmniQueue-Token": TOKEN, "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      state.prediction = await res.json();
      $("#predict-status").textContent = "";
    } catch (err) {
      state.prediction = { error: err.message };
      $("#predict-status").textContent = `cannot estimate (${err.message})`;
    }
    renderPredict();
  }
  function renderPredict() {
    const out = $("#predict-result");
    const r = state.prediction;
    if (!r) { out.replaceChildren(el("p", { class: "muted" }, "Describe the job and press estimate. Candidates are every partition the project poll has load samples for; with GPUs per node > 0 only GPU partitions, otherwise only CPU partitions.")); return; }
    if (r.error) { out.replaceChildren(el("p", { class: "muted" }, `no estimate: ${r.error}`)); return; }
    const frag = document.createDocumentFragment();
    if (!r.candidates.length) frag.append(el("p", { class: "muted" }, "No candidates yet: the project poll has not stored load samples (configure `projects` on a cluster and let `omniqueue monitor` run while logged in)."));
    else {
      const table = el("table", { class: "parts predict-table" },
        el("thead", {}, el("tr", {}, el("th", {}, "#"), el("th", {}, "cluster / partition"), el("th", {}, "project"),
          el("th", { class: "num" }, "est. wait"), el("th", { class: "num" }, "starts at once"), el("th", {}, "confidence"), el("th", {}, "why"))));
      const tb = el("tbody");
      r.candidates.forEach((c, i) => {
        tb.append(el("tr", { class: i === 0 ? "best" : "" },
          el("td", { class: "num" }, i + 1),
          el("td", {}, el("span", { class: "cl", style: `--card-color:${clusterColor(c.cluster)}` }, c.cluster), el("span", { class: "muted" }, ` / ${c.partition}`),
            c.factors?.gpu ? el("span", { class: "array-tag gpu" }, `${c.factors.gpus_per_node || "?"} GPU/node`) : null),
          el("td", { class: "mono" }, c.project || "–"),
          el("td", { class: "num" }, c.estimated_wait_h < 0.05 ? "≈ 0" : c.estimated_wait_h < 1 ? `${Math.round(c.estimated_wait_h * 60)} min` : `${c.estimated_wait_h.toFixed(1)} h`),
          el("td", { class: "num" }, `${Math.round(c.immediate_probability * 100)} %`),
          el("td", {}, el("span", { class: `conf ${c.confidence}` }, c.confidence)),
          el("td", { class: "why" }, c.reasons.join(" · "))));
      });
      table.append(tb);
      frag.append(table);
    }
    if (r.excluded?.length) frag.append(el("p", { class: "muted small" }, "left out: ", r.excluded.map((c) => `${c.cluster}/${c.partition}${c.project ? ` [${c.project}]` : ""} (${c.excluded})`).join(" · ")));
    for (const n of r.notes || []) frag.append(el("p", { class: "muted small" }, n));
    out.replaceChildren(frag);
  }
  // a plain button rather than a form submit: the page's CSP has form-action 'none'
  $("#predict-run").addEventListener("click", runPrediction);
  $("#predict-form").addEventListener("submit", runPrediction);
  $("#predict-form").addEventListener("keydown", (e) => { if (e.key === "Enter") runPrediction(e); });
  $("#predict-back").addEventListener("click", leaveLoadView);
  $("#predict-toggle").addEventListener("click", enterPredictView);

  // ---------- theme ----------
  const THEMES = ["auto", "dark", "light"];
  const THEME_ICON = { auto: "◐", dark: "☾", light: "☀" };
  function currentTheme() { try { return localStorage.getItem("omniqueue.theme") || "auto"; } catch { return "auto"; } }
  function applyTheme(t) {
    if (t === "auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
    try { localStorage.setItem("omniqueue.theme", t); } catch { /* ignore */ }
    $("#theme").textContent = THEME_ICON[t];
    $("#theme").title = `theme: ${t} (t to cycle)`;
  }
  function cycleTheme() { applyTheme(THEMES[(THEMES.indexOf(currentTheme()) + 1) % THEMES.length]); }
  applyTheme(currentTheme());
  $("#theme").addEventListener("click", cycleTheme);

  // ---------- wiring ----------
  $("#refresh").addEventListener("click", requestRefresh);
  $("#search").addEventListener("input", (e) => {
    if (e.target.value.trim().toLowerCase() === "experimental") {  // the magic word toggles the experimental view
      e.target.value = "";
      state.search = "";
      setExperimental(!state.experimental);
      renderTable();
      return;
    }
    state.search = e.target.value; renderTable();
  });
  $("#window").value = String(state.windowHours);
  $("#window").addEventListener("change", (e) => { state.windowHours = Number(e.target.value); savePrefs(); render(); });
  $("#drawer-close").addEventListener("click", closeDrawer);
  function openWidget() {
    window.open("/widget", "omniqueue-widget", "popup=yes,width=380,height=760,menubar=no,toolbar=no,location=no,status=no");
  }
  $("#widget-open").addEventListener("click", openWidget);
  $("#view-toggle").addEventListener("click", () => (state.view === "load" ? refreshLoad() : enterLoadView()));
  $("#load-refresh").addEventListener("click", refreshLoad);
  $("#load-back").addEventListener("click", leaveLoadView);
  $("#tabs").addEventListener("click", (e) => {
    const b = e.target.closest("button[data-tab]");
    if (!b) return;
    state.tab = b.dataset.tab; savePrefs(); render();
  });
  $("#jobs thead").addEventListener("click", (e) => {
    const th = e.target.closest("th[data-sort]");
    if (!th) return;
    const key = th.dataset.sort;
    state.sort = state.sort.key === key ? { key, desc: !state.sort.desc } : { key, desc: key.endsWith("_time") || key.endsWith("_s") };
    savePrefs(); renderTable();
  });
  document.addEventListener("keydown", (e) => {
    if (e.target.matches("input, select, textarea")) { if (e.key === "Escape") e.target.blur(); return; }
    if (e.key === "/") { e.preventDefault(); $("#search").focus(); }
    else if (e.key === "r") requestRefresh();
    else if (e.key === "t") cycleTheme();
    else if (e.key === "l") { if (state.view === "load") refreshLoad(); else enterLoadView(); }
    else if (e.key === "q") leaveLoadView();
    else if (e.key === "x") enterPredictView();
    else if (e.key === "p") { if (state.projects?.enabled) toggleProjects(); }
    else if (e.key === "w") openWidget();
    else if (e.key === "e") toggleAllArrays();
    else if (e.key === "Escape") { if (state.selected) closeDrawer(); else leaveLoadView(); }
    else if ("12345".includes(e.key)) { const b = $$("#tabs button[data-tab]")[Number(e.key) - 1]; if (b) b.click(); }
  });

  // ---------- polling, only while visible ----------
  function startPolling() {
    if (stateTimer) return;
    fetchState();
    stateTimer = setInterval(fetchState, POLL_MS);
    startProjectPolling();
    if (state.view === "load" && state.load?.fetching) startLoadPolling();
  }
  function stopPolling() {
    if (stateTimer) clearInterval(stateTimer);
    stateTimer = null;
    stopLoadPolling();
    stopProjectPolling();
  }
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") startPolling(); else stopPolling();
  });
  window.addEventListener("pagehide", stopPolling);
  if (document.visibilityState === "visible") startPolling();
})();
