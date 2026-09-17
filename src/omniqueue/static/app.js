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
    selected: null,
    error: null,
    matches: new Map(), // job key -> fuzzy match info for the current search
  };

  // ---------- helpers ----------
  function loadPrefs() {
    try { return JSON.parse(localStorage.getItem("omniqueue.prefs") || "{}"); } catch { return {}; }
  }
  function savePrefs() {
    try {
      localStorage.setItem("omniqueue.prefs", JSON.stringify({
        tab: state.tab, sort: state.sort, hiddenClusters: [...state.hiddenClusters], windowHours: state.windowHours,
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
  async function fetchState() {
    try {
      const res = await fetch("/api/state", { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
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
    renderTable();
    if (state.selected) renderDrawer();
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
    rs.textContent = snap.refreshing ? "refreshing…" : `polled ${clock(snap.last_refresh)} · every ${snap.refresh_seconds}s`;
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
        class: `card ${hidden ? "off" : ""} ${c.ok || c.error_kind === "held" ? "" : "err"}`,
        style: `--card-color:${clusterColor(c.name)}`,
        title: hidden ? "click to show this cluster's jobs" : "click to hide this cluster's jobs",
        onclick: () => { hidden ? state.hiddenClusters.delete(c.name) : state.hiddenClusters.add(c.name); savePrefs(); render(); },
      },
        el("div", { class: "card-head" }, logoEl(c), el("b", {}, c.name), el("small", {}, c.host)),
        c.ok
          ? el("div", { class: "card-counts" },
              ...[["running", "running"], ["pending", "pending"], ["problem", "failed"], ["ok", "done"]].map(([k, label]) =>
                el("span", { class: `pill ${k}`, title: label }, el("b", {}, counts[k] || 0), label)))
          : el("div", { class: `card-error ${c.error_kind || ""}` }, el("span", { class: "warn-icon" }, c.error_kind === "held" ? "⏸" : "⚠"),
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
      case "held": return `not polled until you run  omniqueue login ${c.name}`;
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
    for (const b of $$("#tabs button")) {
      b.classList.toggle("active", b.dataset.tab === state.tab);
      $(".count", b).textContent = counts[b.dataset.tab] ?? 0;
    }
  }

  function renderTable() {
    const tbody = $("#jobs tbody");
    const jobs = sortJobs(visibleJobs());
    $("#empty").hidden = jobs.length > 0;
    for (const th of $$("#jobs th")) {
      th.classList.toggle("sorted", th.dataset.sort === state.sort.key);
      th.classList.toggle("desc", th.dataset.sort === state.sort.key && state.sort.desc);
    }
    const rows = jobs.map((j) => {
      const note = j.category === "pending" ? j.reason : (j.exit_summary || (j.category === "unknown" ? j.reason : ""));
      const tr = el("tr", { class: state.selected === j.key ? "selected" : "", "data-key": j.key, onclick: () => openDrawer(j.key) },
        el("td", {}, el("span", { class: "cl", style: `--card-color:${clusterColor(j.cluster)}` }, j.cluster)),
        el("td", { class: "mono" }, highlight(j.job_id, state.matches.get(j.key)?.idPos)),
        el("td", { class: "name", title: j.name }, highlight(j.name, state.matches.get(j.key)?.namePos)),
        el("td", {}, el("span", { class: `state ${j.category}` }, stateLabel(j.state))),
        el("td", { class: "num mono" }, elapsedCell(j)),
        el("td", { class: "num mono" }, fmtDuration(j.time_limit_s)),
        el("td", { class: "num" }, j.nodes || "–"),
        el("td", { class: "mono", title: j.submit_time }, fmtTime(j.submit_time)),
        el("td", { class: "mono", title: j.start_time }, fmtTime(j.start_time)),
        el("td", { class: "mono", title: j.end_time }, fmtTime(j.end_time)),
        el("td", { class: `note ${j.category === "problem" || j.category === "unknown" ? "problem" : ""}`, title: note }, note || ""),
      );
      return tr;
    });
    tbody.replaceChildren(...rows);
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
      ["started", j.start_time || "–"],
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
  $("#search").addEventListener("input", (e) => { state.search = e.target.value; renderTable(); });
  $("#window").value = String(state.windowHours);
  $("#window").addEventListener("change", (e) => { state.windowHours = Number(e.target.value); savePrefs(); render(); });
  $("#drawer-close").addEventListener("click", closeDrawer);
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
    else if (e.key === "Escape") closeDrawer();
    else if ("12345".includes(e.key)) { const b = $$("#tabs button")[Number(e.key) - 1]; if (b) b.click(); }
  });

  fetchState();
  setInterval(fetchState, POLL_MS);
})();
