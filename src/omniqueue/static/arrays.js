/* Shared: group Slurm job-array tasks into one entry per array. Used by app.js and widget.js. */
(() => {
  "use strict";
  const parseT = (t) => { if (!t) return null; const d = new Date(String(t).replace(" ", "T")); return isNaN(d) ? null : d; };
  const minT = (a, b) => (!a ? b : !b ? a : (parseT(a) < parseT(b) ? a : b));
  const maxT = (a, b) => (!a ? b : !b ? a : (parseT(a) > parseT(b) ? a : b));

  /** Returns [{kind:"job", job}] and [{kind:"array", ...}] items. Arrays with a single task row stay plain jobs. */
  function groupArrays(jobs) {
    const groups = new Map();
    const order = [];
    for (const j of jobs) {
      if (!j.array_job_id) { order.push({ kind: "job", job: j, key: j.key }); continue; }
      const gk = `${j.cluster}:${j.array_job_id}`;
      let g = groups.get(gk);
      if (!g) {
        g = { kind: "array", key: gk, cluster: j.cluster, base: j.array_job_id, name: j.name, tasks: [], total: 0,
              counts: { running: 0, pending: 0, ok: 0, problem: 0, unknown: 0 }, submit_time: "", start_time: "", end_time: "",
              time_limit_s: j.time_limit_s, elapsed_max: 0, nodes: j.nodes, partition: j.partition, last_seen: 0, terminal: true, category: "pending" };
        groups.set(gk, g);
        order.push(g);
      }
      g.tasks.push(j);
      const n = j.array_tasks || 1;
      g.total += n;
      g.counts[j.category] = (g.counts[j.category] || 0) + n;
      g.submit_time = minT(g.submit_time, j.submit_time);
      if (j.category === "running" || j.terminal) g.start_time = minT(g.start_time, j.start_time);
      if (j.terminal) g.end_time = maxT(g.end_time, j.end_time);
      g.elapsed_max = Math.max(g.elapsed_max, j.elapsed_s || 0);
      g.last_seen = Math.max(g.last_seen, j.last_seen || 0);
      if (!j.terminal) g.terminal = false;
    }
    for (const g of groups.values()) {
      // the array's own category: running beats pending beats problem beats ok
      g.category = g.counts.running ? "running" : g.counts.pending ? "pending" : g.counts.problem ? "problem" : "ok";
      g.done = g.counts.ok + g.counts.problem;
      g.exit_summary = g.counts.problem ? `${g.counts.problem} task${g.counts.problem === 1 ? "" : "s"} failed` : "";
      if (!g.terminal) g.end_time = "";  // still going: no end yet
    }
    // an "array" of one task row with one task is just a job
    return order.map((it) => (it.kind === "array" && it.tasks.length === 1 && it.total === 1 ? { kind: "job", job: it.tasks[0], key: it.tasks[0].key } : it));
  }

  /** Short human summary of an array's task states, non-zero parts only. */
  function arraySummary(g) {
    const parts = [];
    if (g.counts.running) parts.push(`${g.counts.running} running`);
    if (g.counts.pending) parts.push(`${g.counts.pending} waiting`);
    if (g.counts.ok) parts.push(`${g.counts.ok} done`);
    if (g.counts.problem) parts.push(`${g.counts.problem} failed`);
    return parts.join(" · ");
  }

  window.OQ = Object.assign(window.OQ || {}, { groupArrays, arraySummary });
})();
