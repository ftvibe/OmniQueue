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

  // ---------- data ----------
  async function fetchState() {
    try {
      const res = await fetch("/api/state", { cache: "no-store", headers: etag ? { "If-None-Match": etag } : {} });
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

  // ---------- render ----------
  function render() {
    if (!snapshot) return;
    const jobs = snapshot.jobs;
    const c = { running: 0, pending: 0, ok: 0, problem: 0 };
    for (const j of jobs) if (c[j.category] !== undefined) c[j.category]++;
    const ok = snapshot.clusters.filter((x) => x.ok).length;
    $("#w-summary").textContent = snapshot.offline ? "no cluster reachable" : `${c.running} running · ${c.pending} queued · ${c.problem} failed`;
    $("#w-summary").classList.toggle("offline", !!snapshot.offline);

    // clusters
    const root = $("#w-clusters"); root.replaceChildren();
    for (const cl of snapshot.clusters) {
      const n = cl.counts || {};
      const state = cl.ok ? "ok" : cl.error_kind === "login" ? "login" : "err";
      root.append(el("div", { class: `w-cluster ${state}`, style: `--card-color:${cl.color || "var(--muted)"}`,
          title: cl.ok ? `polled ${clock(cl.last_success)}` : (cl.error || "not reached") },
        el("span", { class: "w-cname" }, cl.name),
        cl.ok
          ? el("span", { class: "w-counts" },
              el("b", { class: "running" }, n.running || 0), el("b", { class: "pending" }, n.pending || 0),
              el("b", { class: "problem" }, n.problem || 0), el("b", { class: "ok" }, n.ok || 0))
          : el("span", { class: "w-cerr" }, state === "login" ? "not logged in" : "unreachable"),
        el("span", { class: `w-dot ${cl.connected === false ? "off" : cl.connected ? "on" : ""}`, title: cl.connected ? "ssh connected" : cl.connected === false ? "ssh not connected" : "" })));
    }

    // alerts
    const probs = problems();
    $("#w-alerts-count").textContent = probs.length ? probs.length : "";
    $("#w-alerts-block").classList.toggle("quiet", probs.length === 0);
    const al = $("#w-alerts"); al.replaceChildren();
    if (!probs.length) al.append(el("li", { class: "w-empty" }, `nothing crashed in the last ${ALERT_HOURS} h`));
    for (const j of probs.slice(0, 12)) {
      al.append(el("li", { class: "w-item alert" },
        el("span", { class: "w-main" }, el("b", {}, j.name), el("span", { class: "w-sub" }, `${j.cluster} · ${j.job_id} · ${fmtWhen(j.end_time)}`)),
        el("span", { class: "w-tag problem" }, j.exit_summary || label(j.state)),
        el("button", { class: "w-x", title: "dismiss", onclick: () => { dismissed.add(j.key); store("omniqueue.widget.dismissed", [...dismissed]); render(); } }, "✕")));
    }

    // recently started
    const started = jobs.filter((j) => j.category === "running" && j.start_time).sort((a, b) => parseT(b.start_time) - parseT(a.start_time));
    fillList($("#w-started"), started, (j) => [`started ${fmtWhen(j.start_time)}`, el("span", { class: "w-tag running" }, `${fmtDur(j.elapsed_s)} / ${fmtDur(j.time_limit_s)}`)], "no running jobs");
    $("#w-started-count").textContent = started.length ? `${started.length} running` : "";

    // recently finished (completed only; problems live in alerts)
    const finished = jobs.filter((j) => j.category === "ok" && j.end_time).sort((a, b) => parseT(b.end_time) - parseT(a.end_time));
    fillList($("#w-finished"), finished, (j) => [`finished ${fmtWhen(j.end_time)}`, el("span", { class: "w-tag ok" }, `took ${fmtDur(j.elapsed_s)}`)], "nothing finished yet");
    $("#w-finished-count").textContent = "";

    $("#w-status").textContent = `polled ${clock(snapshot.last_refresh)} · widget re-reads every ${Math.round(REFRESH_S / 60)} min`;
  }
  function fillList(ul, items, extra, emptyText) {
    ul.replaceChildren();
    if (!items.length) { ul.append(el("li", { class: "w-empty" }, emptyText)); return; }
    for (const j of items.slice(0, LIMIT)) {
      const [sub, tag] = extra(j);
      ul.append(el("li", { class: "w-item" },
        el("span", { class: "w-main" }, el("b", {}, j.name), el("span", { class: "w-sub" }, `${j.cluster} · ${j.job_id} · ${sub}`)), tag));
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
  $("#w-clear").addEventListener("click", () => { for (const j of problems()) dismissed.add(j.key); store("omniqueue.widget.dismissed", [...dismissed]); render(); });
  $("#w-notify").addEventListener("click", async () => { if ("Notification" in window && Notification.permission === "default") await Notification.requestPermission(); updateBell(); });
  updateBell();
  function start() { if (timer) return; fetchState(); timer = setInterval(fetchState, REFRESH_S * 1000); }
  function stop() { if (timer) clearInterval(timer); timer = null; }
  document.addEventListener("visibilitychange", () => (document.visibilityState === "visible" ? start() : stop()));
  if (document.visibilityState === "visible") start();
})();
