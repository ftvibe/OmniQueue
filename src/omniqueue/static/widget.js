/* OmniQueue side widget: compact summary, alerts for crashed jobs, recent starts/finishes.
   Re-reads the server snapshot every REFRESH_S seconds (?refresh=600), only while visible. */
(() => {
  "use strict";
  const $ = (s, r = document) => r.querySelector(s);
  const params = new URLSearchParams(location.search);
  const REFRESH_S = Math.max(30, Number(params.get("refresh")) || 600);
  const ALERT_HOURS = Math.max(1, Number(params.get("alerts")) || 48);
  const LIMIT = Math.max(3, Number(params.get("n")) || 6);
  const TOKEN = $('meta[name="omniqueue-token"]')?.content || "";
  const pad = (n) => String(n).padStart(2, "0");

  let snapshot = null, etag = null, timer = null;
  let mode = "summary"; // "summary" | "running"
  const expanded = new Set(); // array groups unfolded in running mode
  const CLIENT_ID = "w-" + Math.random().toString(36).slice(2);
  const store = (k, v) => { try { v === undefined ? localStorage.removeItem(k) : localStorage.setItem(k, JSON.stringify(v)); } catch { /* ignore */ } };
  const load = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch { return d; } };
  let dismissed = new Set(load("omniqueue.widget.dismissed", []));
  let seen = new Set(load("omniqueue.widget.seen", []));
  let firstLoad = load("omniqueue.widget.seen", null) === null;

  function el(tag, attrs = {}, ...children) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") e.className = v; else if (k === "style") e.style.cssText = v;
      else if (k.startsWith("on")) e.addEventListener(k.slice(2), v); else if (v != null) e.setAttribute(k, v);
    }
    for (const c of children.flat()) if (c != null) e.append(c.nodeType ? c : String(c));
    return e;
  }
  const parseT = (t) => { if (!t) return null; const d = new Date(t.replace(" ", "T")); return isNaN(d) ? null : d; };
  function fmtWhen(t) {
    const d = parseT(t); if (!d) return "–";
    const now = new Date(), diff = (now - d) / 1000;
    if (diff < 3600) return `${Math.max(1, Math.round(diff / 60))} min ago`;
    if (d.toDateString() === now.toDateString()) return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
    return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }
  function fmtDur(s) {
    if (s == null) return "–";
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
    return h >= 24 ? `${Math.floor(h / 24)}d ${h % 24}h` : h ? `${h}h ${pad(m)}m` : `${m}m`;
  }
  const clock = (u) => { const d = new Date(u * 1000); return `${pad(d.getHours())}:${pad(d.getMinutes())}`; };
  const STATE_LABEL = { FAILED: "failed", TIMEOUT: "timed out", OUT_OF_MEMORY: "out of memory", CANCELLED: "cancelled",
    NODE_FAIL: "node failure", PREEMPTED: "preempted", BOOT_FAIL: "boot failure", DEADLINE: "deadline", VANISHED: "vanished", COMPLETED: "completed" };
  const label = (s) => STATE_LABEL[s] || s.toLowerCase().replace(/_/g, " ");

  // cluster colours: from the config, else a fixed palette in cluster order
  const AUTO = ["#4f8f8a", "#e2856c", "#b39a4b", "#a9cbc8", "#3a615b", "#f3c6b6", "#c8b47c", "#073a34"];
  function clusterColor(name) {
    const i = snapshot.clusters.findIndex((c) => c.name === name);
    return (i >= 0 && snapshot.clusters[i].color) || AUTO[Math.max(i, 0) % AUTO.length];
  }
  function inkFor(hex) {  // dark or light text on a coloured pill
    const m = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex || "");
    if (!m) return "#fff";
    const [r, g, b] = [1, 2, 3].map((k) => parseInt(m[k], 16) / 255);
    return 0.2126 * r + 0.7152 * g + 0.0722 * b > 0.55 ? "#0d3b38" : "#fbefdc";
  }
  const clusterPill = (name) => el("span", { class: "w-cpill", style: `background:${clusterColor(name)};color:${inkFor(clusterColor(name))}` }, name);

  // ---------- data ----------
  async function fetchState() {
    try {
      // tell the server how often this widget re-reads: with only widgets open it polls the clusters at that rate
      const headers = { "X-OmniQueue-Client": CLIENT_ID, "X-OmniQueue-Interval": String(REFRESH_S) };
      if (etag) headers["If-None-Match"] = etag;
      const res = await fetch("/api/state", { cache: "no-store", headers });
      if (res.status === 304) return;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      etag = res.headers.get("ETag");
      snapshot = await res.json();
      render();
      notifyNew();
    } catch (err) {
      $("#w-status").textContent = `cannot reach OmniQueue (${err.message})`;
    }
  }
  async function requestRefresh() {
    try { await fetch("/api/refresh", { method: "POST", headers: { "X-OmniQueue-Token": TOKEN } }); } catch { /* ignore */ }
    setTimeout(fetchState, 1500);
    setTimeout(fetchState, 6000);
  }

  function problems() {
    const cutoff = Date.now() - ALERT_HOURS * 3600e3;
    return snapshot.jobs.filter((j) => (j.category === "problem" || j.category === "unknown") && !dismissed.has(j.key))
      .filter((j) => { const t = parseT(j.end_time) || new Date(j.last_seen * 1000); return t >= cutoff; })
      .sort((a, b) => (parseT(b.end_time) || 0) - (parseT(a.end_time) || 0));
  }

  // ---------- running mode: every running job with a progress bar ----------
  function renderRunning() {
    const ul = $("#w-running-list"); ul.replaceChildren();
    // arrays with running tasks appear once, with overall progress; plain jobs by time used
    const active = snapshot.jobs.filter((j) => j.category === "running" || j.array_job_id);
    const items = OQ.groupArrays(active).filter((it) => it.kind === "job" ? it.job.category === "running" : it.counts.running > 0);
    const runningCount = snapshot.jobs.filter((j) => j.category === "running").length;
    $("#w-running-count").textContent = runningCount ? `${runningCount} job${runningCount === 1 ? "" : "s"}` : "";
    if (!items.length) { ul.append(el("li", { class: "w-empty" }, "no running jobs")); return; }
    const arrays = items.filter((it) => it.kind === "array");
    for (const g of arrays) {
      const c = g.counts, donePct = g.total ? c.ok / g.total * 100 : 0, failPct = g.total ? c.problem / g.total * 100 : 0, runPct = g.total ? c.running / g.total * 100 : 0;
      const open = expanded.has(g.key);
      const toggle = () => { if (open) expanded.delete(g.key); else expanded.add(g.key); renderRunning(); };
      const main = el("span", { class: "w-main" },
        el("span", { class: "w-runhead" }, el("span", {}, el("span", { class: "w-caret" }, open ? "▾" : "▸"), el("b", {}, g.name), el("span", { class: "w-array" }, `array ${g.base}`)),
          el("span", { class: "w-pct" }, `${g.done}/${g.total}`)),
        el("span", { class: "w-bar split", title: `${c.ok} done · ${c.problem} failed · ${c.running} running · ${c.pending} waiting` },
          el("i", { class: "done", style: `width:${donePct.toFixed(1)}%` }),
          el("i", { style: `width:${failPct.toFixed(1)}%;background:var(--problem)` }),
          el("i", { class: "running", style: `width:${runPct.toFixed(1)}%` })),
        el("span", { class: "w-sub" }, clusterPill(g.cluster), ` ${OQ.arraySummary(g)}`));
      if (open) {  // thin bars, one per running task, closest to the limit first
        const tasks = g.tasks.filter((t) => t.category === "running")
          .map((t) => ({ t, frac: t.time_limit_s ? Math.min(1, (t.elapsed_s || 0) / t.time_limit_s) : 0 })).sort((a, b) => b.frac - a.frac);
        const list = el("span", { class: "w-tasks" });
        for (const { t, frac } of tasks) {
          const pct = Math.round(frac * 100);
          list.append(el("span", { class: "w-task", title: `task ${t.job_id.split("_")[1]} · ${fmtDur(t.elapsed_s)} of ${fmtDur(t.time_limit_s)}${t.node_list ? " · " + t.node_list : ""}` },
            el("span", { class: "w-tid" }, t.job_id.split("_")[1] ?? ""),
            el("span", { class: `w-thin ${pct >= 90 ? "hot" : ""}` }, el("i", { style: `width:${pct}%` })),
            el("span", { class: `w-tpct ${pct >= 90 ? "hot" : ""}` }, `${pct}%`)));
        }
        if (c.pending) list.append(el("span", { class: "w-waiting" }, `${c.pending} task${c.pending === 1 ? "" : "s"} waiting to start`));
        main.append(list);
      }
      ul.append(el("li", { class: `w-item w-run w-arr ${open ? "open" : ""}`, style: `--card-color:${clusterColor(g.cluster)}`, onclick: toggle,
        title: open ? "click to fold the tasks" : "click to unfold the running tasks (e: all)" }, main));
    }
    const running = items.filter((it) => it.kind === "job").map(({ job: j }) => ({ j, frac: j.time_limit_s ? Math.min(1, (j.elapsed_s || 0) / j.time_limit_s) : 0 }))
      .sort((a, b) => b.frac - a.frac || (a.j.time_limit_s || 0) - (b.j.time_limit_s || 0));
    for (const { j, frac } of running) {
      const pct = Math.round(frac * 100);
      const left = j.time_limit_s ? fmtDur(Math.max(0, j.time_limit_s - (j.elapsed_s || 0))) + " left" : "no limit";
      ul.append(el("li", { class: "w-item w-run", style: `--card-color:${clusterColor(j.cluster)}` },
        el("span", { class: "w-main" },
          el("span", { class: "w-runhead" }, el("b", {}, j.name), el("span", { class: `w-pct ${pct >= 90 ? "hot" : ""}` }, j.time_limit_s ? `${pct}%` : "")),
          el("span", { class: `w-bar ${pct >= 90 ? "hot" : ""}`, title: `${fmtDur(j.elapsed_s)} of ${fmtDur(j.time_limit_s)}` }, el("i", { style: `width:${pct}%` })),
          el("span", { class: "w-sub" }, clusterPill(j.cluster), ` ${j.job_id} · ${fmtDur(j.elapsed_s)} / ${fmtDur(j.time_limit_s)} · ${left}${j.nodes ? ` · ${j.nodes} node${j.nodes === 1 ? "" : "s"}` : ""}`))));
    }
  }
  function setMode(m) {
    mode = m;
    $("#w-running-block").hidden = mode !== "running";
    $("#w-summary-blocks").hidden = mode === "running";
    $("#w-running").classList.toggle("on", mode === "running");
    if (snapshot) render();
  }

  // ---------- render ----------
  function render() {
    if (!snapshot) return;
    if (mode === "running") renderRunning();
    const jobs = snapshot.jobs;
    const c = { running: 0, pending: 0, ok: 0, problem: 0 };
    for (const j of jobs) if (c[j.category] !== undefined) c[j.category]++;
    const ok = snapshot.clusters.filter((x) => x.ok).length;
    $("#w-summary").textContent = snapshot.offline ? "no cluster reachable" : `${c.running} running · ${c.pending} queued · ${c.problem} failed`;
    $("#w-summary").classList.toggle("offline", !!snapshot.offline);

    // clusters, with a header row naming the four columns
    const root = $("#w-clusters"); root.replaceChildren();
    root.append(el("div", { class: "w-chead" },
      el("span", { class: "w-cname" }, "cluster"),
      el("span", { class: "w-counts" },
        el("span", { class: "running", title: "running now" }, "run"), el("span", { class: "pending", title: "waiting in the queue" }, "queue"),
        el("span", { class: "problem", title: "failed, timed out, out of memory, cancelled" }, "fail"), el("span", { class: "ok", title: "completed" }, "done")),
      el("span", { class: "w-dot", style: "visibility:hidden" })));
    for (const cl of snapshot.clusters) {
      const n = cl.counts || {};
      const state = cl.ok ? "ok" : cl.error_kind === "login" ? "login" : "err";
      root.append(el("div", { class: `w-cluster ${state}`, style: `--card-color:${clusterColor(cl.name)}`,
          title: cl.ok ? `polled ${clock(cl.last_success)}` : (cl.error || "not reached") },
        el("span", { class: "w-cname" }, clusterPill(cl.name)),
        cl.ok
          ? el("span", { class: "w-counts" },
              el("b", { class: `running ${n.running ? "" : "zero"}`, title: `${n.running || 0} running` }, n.running || 0),
              el("b", { class: `pending ${n.pending ? "" : "zero"}`, title: `${n.pending || 0} waiting in the queue` }, n.pending || 0),
              el("b", { class: `problem ${n.problem ? "" : "zero"}`, title: `${n.problem || 0} failed / timed out / cancelled` }, n.problem || 0),
              el("b", { class: `ok ${n.ok ? "" : "zero"}`, title: `${n.ok || 0} completed` }, n.ok || 0))
          : el("span", { class: "w-cerr" }, state === "login" ? "not logged in" : "unreachable"),
        el("span", { class: `w-dot ${cl.connected === false ? "off" : cl.connected ? "on" : ""}`, title: cl.connected ? "ssh connected" : cl.connected === false ? "ssh not connected" : "" })));
    }

    // alerts, failed array tasks grouped per array
    const probs = problems();
    const alertItems = OQ.groupArrays(probs);
    $("#w-alerts-count").textContent = probs.length ? probs.length : "";
    $("#w-alerts-block").classList.toggle("quiet", probs.length === 0);
    const al = $("#w-alerts"); al.replaceChildren();
    if (!probs.length) al.append(el("li", { class: "w-empty" }, `nothing crashed in the last ${ALERT_HOURS} h`));
    for (const it of alertItems.slice(0, 12)) {
      if (it.kind === "job") {
        const j = it.job;
        al.append(el("li", { class: "w-item alert", style: `--card-color:${clusterColor(j.cluster)}` },
          el("span", { class: "w-main" }, el("b", {}, j.name), el("span", { class: "w-sub" }, clusterPill(j.cluster), ` ${j.job_id} · ${fmtWhen(j.end_time)}`)),
          el("span", { class: "w-tag problem" }, j.exit_summary || label(j.state)),
          el("button", { class: "w-x", title: "dismiss", onclick: () => { dismissed.add(j.key); store("omniqueue.widget.dismissed", [...dismissed]); render(); } }, "✕")));
      } else {
        const g = it, reasons = {};
        for (const t of g.tasks) { const r = t.exit_summary || label(t.state); reasons[r] = (reasons[r] || 0) + (t.array_tasks || 1); }
        const why = Object.entries(reasons).sort((a, b) => b[1] - a[1]).map(([r, n]) => n > 1 ? `${r} ×${n}` : r).join(", ");
        const tasks = g.tasks.map((t) => t.job_id.split("_")[1]).slice(0, 6).join(",") + (g.tasks.length > 6 ? ",…" : "");
        al.append(el("li", { class: "w-item alert", style: `--card-color:${clusterColor(g.cluster)}` },
          el("span", { class: "w-main" }, el("span", {}, el("b", {}, g.name), el("span", { class: "w-array" }, `array ${g.base}`)),
            el("span", { class: "w-sub" }, clusterPill(g.cluster), ` tasks ${tasks} · ${fmtWhen(g.end_time)}`)),
          el("span", { class: "w-tag problem", title: why }, `${g.total} task${g.total === 1 ? "" : "s"} failed`),
          el("button", { class: "w-x", title: "dismiss all", onclick: () => { for (const t of g.tasks) dismissed.add(t.key); store("omniqueue.widget.dismissed", [...dismissed]); render(); } }, "✕")));
      }
    }

    // recently started: arrays as one entry with their running task count
    const runningJobs = jobs.filter((j) => j.category === "running" && j.start_time);
    const started = OQ.groupArrays(runningJobs).sort((a, b) => latest(b, "start_time") - latest(a, "start_time"));
    fillList($("#w-started"), started, (it) => it.kind === "job"
      ? [`started ${fmtWhen(it.job.start_time)}`, el("span", { class: "w-tag running" }, `${fmtDur(it.job.elapsed_s)} / ${fmtDur(it.job.time_limit_s)}`)]
      : [`${it.tasks.length} task${it.tasks.length === 1 ? "" : "s"} running · latest ${fmtWhen(latestTime(it, "start_time"))}`,
         el("span", { class: "w-tag running" }, `${it.tasks.length} × ≤ ${fmtDur(it.time_limit_s)}`)], "no running jobs");
    $("#w-started-count").textContent = runningJobs.length ? `${runningJobs.length} running` : "";

    // recently finished (completed only; problems live in alerts), arrays grouped
    const finishedJobs = jobs.filter((j) => j.category === "ok" && j.end_time);
    const finished = OQ.groupArrays(finishedJobs).sort((a, b) => latest(b, "end_time") - latest(a, "end_time"));
    fillList($("#w-finished"), finished, (it) => it.kind === "job"
      ? [`finished ${fmtWhen(it.job.end_time)}`, el("span", { class: "w-tag ok" }, `took ${fmtDur(it.job.elapsed_s)}`)]
      : [`${it.tasks.length} task${it.tasks.length === 1 ? "" : "s"} done · latest ${fmtWhen(latestTime(it, "end_time"))}`,
         el("span", { class: "w-tag ok" }, `≤ ${fmtDur(it.elapsed_max)} each`)], "nothing finished yet");
    $("#w-finished-count").textContent = "";

    const eff = Math.round((snapshot.effective_refresh || snapshot.refresh_seconds) / 60);
    $("#w-status").textContent = `polled ${clock(snapshot.last_refresh)} · clusters polled every ${eff} min · widget re-reads every ${Math.round(REFRESH_S / 60)} min`;
    $("#w-signout").hidden = !snapshot.protected;
  }
  function latestTime(it, field) {
    if (it.kind === "job") return it.job[field];
    return it.tasks.map((t) => t[field]).filter(Boolean).sort((a, b) => parseT(b) - parseT(a))[0] || "";
  }
  const latest = (it, field) => parseT(latestTime(it, field)) || 0;
  function fillList(ul, items, extra, emptyText) {
    ul.replaceChildren();
    if (!items.length) { ul.append(el("li", { class: "w-empty" }, emptyText)); return; }
    for (const it of items.slice(0, LIMIT)) {
      const [sub, tag] = extra(it);
      const name = it.kind === "job" ? it.job.name : it.name;
      const cluster = it.kind === "job" ? it.job.cluster : it.cluster;
      const id = it.kind === "job" ? it.job.job_id : `${it.base}_[…]`;
      ul.append(el("li", { class: "w-item", style: `--card-color:${clusterColor(cluster)}` },
        el("span", { class: "w-main" }, el("span", {}, el("b", {}, name), it.kind === "array" ? el("span", { class: "w-array" }, "array") : null),
          el("span", { class: "w-sub" }, clusterPill(cluster), ` ${id} · ${sub}`)), tag));
    }
  }

  // ---------- notifications ----------
  function notifyNew() {
    const probs = problems();
    const fresh = probs.filter((j) => !seen.has(j.key));
    for (const j of probs) seen.add(j.key);
    store("omniqueue.widget.seen", [...seen]);
    if (firstLoad) { firstLoad = false; return; } // don't announce history on the first open
    if (!fresh.length || !("Notification" in window) || Notification.permission !== "granted") return;
    const body = fresh.slice(0, 4).map((j) => `${j.cluster}: ${j.name} ${j.exit_summary || label(j.state)}`).join("\n") + (fresh.length > 4 ? `\n+${fresh.length - 4} more` : "");
    try { new Notification(fresh.length === 1 ? "OmniQueue: job failed" : `OmniQueue: ${fresh.length} jobs failed`, { body, icon: "/favicon.svg", tag: "omniqueue-alerts" }); } catch { /* ignore */ }
  }
  function updateBell() {
    const b = $("#w-notify");
    if (!("Notification" in window)) { b.hidden = true; return; }
    const p = Notification.permission;
    b.textContent = p === "granted" ? "🔔" : "🔕";
    b.title = p === "granted" ? "notifications on" : p === "denied" ? "notifications blocked in the browser" : "click to enable notifications for crashed jobs";
    b.classList.toggle("on", p === "granted");
  }

  // ---------- wiring ----------
  $("#w-refresh").addEventListener("click", requestRefresh);
  $("#w-running").addEventListener("click", () => setMode(mode === "running" ? "summary" : "running"));
  $("#w-running-back").addEventListener("click", () => setMode("summary"));
  document.addEventListener("keydown", (e) => {
    if (e.target.matches("input, select, textarea")) return;
    if (e.key === "r") setMode(mode === "running" ? "summary" : "running");
    else if (e.key === "q" || e.key === "Escape") setMode("summary");
    else if (e.key === "e" && mode === "running" && snapshot) {
      const keys = OQ.groupArrays(snapshot.jobs).filter((it) => it.kind === "array" && it.counts.running).map((it) => it.key);
      if (keys.some((k) => expanded.has(k))) expanded.clear(); else for (const k of keys) expanded.add(k);
      renderRunning();
    }
  });
  $("#w-clear").addEventListener("click", () => { for (const j of problems()) dismissed.add(j.key); store("omniqueue.widget.dismissed", [...dismissed]); render(); });
  $("#w-notify").addEventListener("click", async () => { if ("Notification" in window && Notification.permission === "default") await Notification.requestPermission(); updateBell(); });
  updateBell();
  function start() { if (timer) return; fetchState(); timer = setInterval(fetchState, REFRESH_S * 1000); }
  function stop() { if (timer) clearInterval(timer); timer = null; }
  document.addEventListener("visibilitychange", () => (document.visibilityState === "visible" ? start() : stop()));
  if (document.visibilityState === "visible") start();
})();
