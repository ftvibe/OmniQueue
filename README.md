<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="src/omniqueue/static/logo_text_dark.svg">
    <img src="src/omniqueue/static/logo_text.svg" width="420" alt="OmniQueue">
  </picture>
</p>

One dashboard for your Slurm jobs on several supercomputers.

OmniQueue logs into each cluster over ssh, asks `squeue` and `sacct` what your
jobs are doing, merges the answers with a local history, and shows the result in
a browser dashboard that looks and feels like the HyperQueue monitor:

* which jobs are **running** where, how long they have been running and how
  much of their time limit is left,
* which jobs are still **queueing**, and why Slurm is holding them (`Priority`,
  `Resources`, `Dependency`, ...),
* which jobs **finished**, how long they took, and
* which jobs **crashed**: failed, timed out, ran out of memory, were cancelled,
  or lost their node, with the exit code and a plain-language note.

Python (standard library only) does the collecting; the GUI is plain HTML/JS
served from the same process. There is nothing to install besides Python 3.11+
and a working `ssh`.

## Quick start

```sh
pip install -e .            # or: pipx install .
omniqueue init              # writes ~/.config/omniqueue/config.toml
$EDITOR ~/.config/omniqueue/config.toml
omniqueue login             # open one ssh connection per cluster (password / 2FA ok)
omniqueue monitor           # start polling and open http://127.0.0.1:8765/
```

Try it without any cluster access:

```sh
omniqueue --demo monitor
```

## Configuration

`~/.config/omniqueue/config.toml`:

```toml
# mode = "pi"             # "user": your jobs and your usage per project; "pi": also whole projects
refresh_seconds = 60      # how often every cluster is polled
lookback_hours  = 72      # how far back sacct is asked for finished jobs (the first poll takes history_days)
history_days    = 30      # finished jobs stay in the local history this long
ssh_timeout     = 20
persist_connections = true   # keep one ssh connection per cluster open between polls
persist_seconds = 14400      # ... for this long after the last poll (4 h)
accept_new_host_keys = false # polls require hosts in known_hosts; `omniqueue login` verifies new ones
keepalive_seconds = 15       # notice a dead connection within ~45 s
retry_seconds   = 15         # retry failed clusters after 15 s, 30 s, 60 s ... up to refresh_seconds
project_refresh_seconds = 7200  # slow background poll of project usage / fairshare / load samples (2 h)
project_history_days = 90       # how long project jobs and load samples are kept
project_timeout = 300           # one project poll may take this long (sacct over all users is slow)
project_backfill_days = 7       # older history arrives in chunks of this many days after the first poll
project_overlap_hours = 24      # each poll re-reads this much before the previous poll (late accounting)
listen_host     = "127.0.0.1"
listen_port     = 8765

[[clusters]]
name = "tetralith"
host = "tetralith"          # anything ssh accepts, aliases from ~/.ssh/config included
# user = "x_flotr"          # your account there: ssh login and Slurm user; default: your local username
# ssh_options = ["-J", "bastion", "-i", "~/.ssh/id_omniqueue"]   # allow-listed flags only, see Security notes
# squeue_args = ["--partition=main"]
# sacct_args  = ["--account=naiss2024-1-23"]
# use_sacct = false         # for clusters without job accounting
# color = "#5f9e99"         # accent colour in the dashboard
# logo = "~/Pictures/nsc.png" # or drop <name>.svg/.png into ~/.config/omniqueue/logos/
# projects = ["naiss2025-1-23"]                    # watch these Slurm accounts: who runs how much (all users)
# project_quotas = { "naiss2025-1-23" = 100000 }  # core-hours per 30 days, if sshare does not publish a limit
# project_gpu_quotas = { "naiss2025-1-23" = 2000 } # GPU-hours per 30 days; GPU jobs are kept apart from CPU jobs
# project_pis = { "naiss2025-1-23" = "A. Nilsson" }   # PI (or any label) shown next to the project name
# gpu_partitions = ["gpu"]                         # which partitions are GPU ones; default: those sinfo reports GPUs for
# gpu_hour_factor = 0.5                            # GPU-hours per Slurm GPU unit and hour (LUMI-G: 8 units per node for 4 MI250X)
# project_refresh_seconds = 3600                  # poll this cluster's projects hourly instead of the global 2 h
# nice = 0                  # the --nice you usually submit with here (the experimental predictor accounts for it)

[[clusters]]
name = "dardel"
host = "dardel.pdc.kth.se"
user = "flotr"
```

## ssh connections

OmniQueue does not keep its own streams open. Instead it relies on ssh
connection multiplexing (`ControlMaster`): the first connection to a cluster
becomes a *master* that stays in the background, and every later poll is a
cheap new session over that existing connection, with no TCP handshake, key
exchange or login prompt. The master closes itself after `persist_seconds`
(default 4 h) without use. One poll is exactly one ssh session per cluster:
`squeue` and `sacct` run in the same remote shell.

The workflow is always **login, monitor, logout**. Polls never open a
connection themselves: a cluster without one shows "not logged in" in the
dashboard until you run `omniqueue login` for it, and `omniqueue logout`
closes the connections and stops the polling again. Nothing reconnects behind
your back. (Set `connect_on_poll = true` if you would rather have key-based
clusters reconnect automatically.)

Open the connections:

```sh
omniqueue login            # all clusters that are not connected yet
omniqueue login dardel     # just one
omniqueue active           # which connections are open, and for how much longer
omniqueue logout           # close all connections now
omniqueue logout dardel    # close one
```

You type the password/OTP once where needed; the poller reuses that connection
afterwards.
Ctrl-C during `login` cancels the attempt cleanly. `logout` first asks the
master to exit; a master hung on a dead link ignores that, so it is then
killed and its socket removed, and `login` does the same clean-up before
connecting if it finds a dead socket. Your own `ssh host` in a terminal does
not share these connections: OmniQueue keeps its sockets under
`~/.local/share/omniqueue/ssh/`, separate from anything in `~/.ssh/config`.
Set `persist_connections = false` to fall back to a fresh ssh per poll (keys or
an agent are then required). The sockets live in `~/.local/share/omniqueue/ssh/`.

### On a laptop that changes networks

Switching from wifi to a phone hotspot, or closing the lid, kills the TCP
connection underneath an ssh master without telling it. OmniQueue copes:

* Every connection runs with `ServerAliveInterval` (default 15 s, three misses
  allowed), so a master on a dead link exits by itself within about 45 s and
  the next poll opens a fresh one.
* A poll that times out or hits a network error closes that cluster's master
  immediately instead of waiting for the keepalive.
* While any cluster is failing, polls retry after `retry_seconds` (15 s),
  doubling each time up to the normal `refresh_seconds`, so you are back within
  seconds of the network returning rather than a full interval later.
* The card says what went wrong: a network problem ("are you online?"),
  a dropped connection, or a login that needs your password / 2FA again, in
  which case it tells you to run `omniqueue login <cluster>`. When no cluster
  is reachable the header says so and the table keeps showing the last known
  jobs from the local history.

Clusters that use keys or an agent reconnect fully automatically. Clusters that
ask for a one-time code need `omniqueue login <cluster>` once after each
network change that dropped the connection.

## Commands

| command | what it does |
|---|---|
| `omniqueue monitor [--port N]` | poll all clusters in the background, serve the dashboard and open it |
| `omniqueue monitor widget [--plain]` | the same, but open only the side widget; on macOS in a small Safari window docked at the right (`--plain` for a normal tab) |
| `omniqueue serve [--open]` | the same without opening a browser (for a headless machine) |
| `omniqueue login [CLUSTER...]` | open the persistent ssh connection, allowing password / 2FA |
| `omniqueue active` | list the open ssh connections, their PIDs, last use and when they close |
| `omniqueue logout [CLUSTER...]` | close the persistent ssh connection(s) now |
| `omniqueue login --force CLUSTER` | reconnect a cluster whose connection is stale |
| `omniqueue list [--state running] ...` | poll once and print a table to the terminal |
| `omniqueue projects [--poll]` | who runs how much in your projects, from the stored slow poll (`--poll` asks the clusters now) |
| `omniqueue predict -N 4 -t 12 [-G 4] [-A proj]` | experimental: rank clusters/partitions by estimated queue wait for such a job (`-G` GPUs per node: GPU partitions only) |
| `omniqueue check` | connect to every cluster once and report problems |
| `omniqueue init [--force]` | write the example config |
| `omniqueue completion bash\|zsh\|fish` | print a tab-completion script |
| `omniqueue --demo ...` | run any command against fabricated clusters |
| `omniqueue --config PATH ...` | use another config file |

Dashboard keys: `/` search, `r` refresh now, `l` cluster load, `q` back to jobs, `e` expand/collapse arrays, `w` side widget, `t` theme, `1`-`5` tabs, `u` collapse/expand my usage, `p` collapse/expand the project cards, `x` where to submit (after unlocking, see below), `Esc` close.

## Tab completion

```sh
# bash: add to ~/.bashrc
eval "$(omniqueue completion bash)"
# zsh: add to ~/.zshrc, after `autoload -Uz compinit && compinit`
eval "$(omniqueue completion zsh)"
# fish
omniqueue completion fish > ~/.config/fish/completions/omniqueue.fish
```

Completes the subcommands and their flags; `login` and `logout` complete the
cluster names from your config, `list --state` the job states.

## The dashboard

* **Cluster cards** show running / pending / failed / done counts, the time of
  the last successful poll, and the ssh or Slurm error if the cluster could not
  be reached. Click a card to hide or show that cluster's jobs.
* **Logos**: drop `<cluster name>.svg` (or `.png`, `.jpg`, `.webp`) into
  `~/.config/omniqueue/logos/` and it appears on the card, or set
  `logo = "path-or-https-url"` in the cluster entry. Without one, the card shows
  the cluster's initials in its accent colour.
* Nothing on the page ticks between polls: elapsed times and timestamps are
  those of the last poll, so the layout stays still. The page asks the server
  for news every few seconds with the version it already has and gets an
  empty 304 unless a poll finished, re-renders only then, and stops asking
  altogether while the tab or window is hidden.
* **Tabs** filter by category. The search box matches loosely: `vsp13`
  finds `vasp-relax-13`, `6195` finds job `619506`. Matched characters are
  highlighted and results are ranked by match quality while you type.
  Several space-separated terms must all match; node list, reason, partition,
  account and work directory are searched too (plain substring).
* **Running jobs** show the elapsed time at the last poll and a bar of the time
  limit used. The bar turns coral past 90 %.
* **Failed jobs** carry a note such as `exit code 1`, `hit time limit`,
  `out of memory` or `cancelled by uid 1234`.
* **Cluster load** (`l`, or the button at the right of the tabs) swaps your
  jobs for a view of each cluster: CPU utilisation, and per partition the node
  states as a bar (idle / mixed / allocated / down), free nodes and free
  physical cores, the time limit, and the queue pressure from *all* users:
  running jobs, queued jobs and how many nodes they need. Partitions are
  listed in two groups, CPU and GPU (those `sinfo` reports a `gpu` gres for);
  the GPU group adds a free GPUs column, counting the GPUs of fully idle nodes
  against all GPUs in the partition. Queued jobs counts
  array tasks individually, and the nodes needed take the larger of a job's
  node request and its CPU request divided by the partition's CPUs per node,
  since a job submitted with `-n` alone reports one node. Free nodes counts
  fully idle nodes; free cores also includes the empty cores of mixed
  nodes. On clusters where Slurm counts hardware threads as CPUs (LUMI, for
  instance, reports 256 per 128-core node) the figures are divided by the
  threads per core and a note says so, so you always read cores. Free nodes
  in bold means you can probably start right away; a queue with nodes wanted
  and no free nodes means a wait. `q` (or Esc) returns to your jobs.
  The load is fetched **on demand only**: when you enter the view, when you
  press `l` again, or with its refresh button. The regular poll never runs
  `sinfo`. Per cluster, `load_partitions = ["main", "gpu"]` limits the view
  to the partitions you care about and `show_load = false` leaves the cluster
  out.
* Pending jobs show Slurm's estimated start time in the Started column,
  italic with a `~`, whenever the backfill scheduler has computed one (the
  same value `squeue --start` prints). Jobs the scheduler has not evaluated
  yet show none.
* **Job arrays** appear as one row per array: `620000_[40]`, the name with an
  "array · 40" tag, badges for how many tasks are running, waiting, done and
  failed, and a bar of finished tasks. Click the row to unfold the individual
  tasks; click a task for its details. The widget groups arrays the same way:
  one alert per array listing the failed task ids and reasons, one entry in the
  recent lists, and in running mode one bar split into done / failed / running
  with "5 running · 25 waiting · 8 done · 2 failed"; click it (or press `e` for
  all) to unfold thin bars for each running task. `e` in the dashboard expands
  or collapses every array.
* Click a row for all details (queue wait, node list, work dir, exit code, ...).
  Finished jobs can be removed from the local history from there.
* **My usage** sits below the cluster cards: one card per project (Slurm
  account) you have jobs in, computed from your own job history with no extra
  cluster access. CPU jobs and GPU jobs are kept apart: *cpu now / cpu 30 d*
  in cores and core-hours, *gpu now / gpu 30 d* in GPUs and GPU-hours (only on
  clusters with GPU partitions), a per-day chart with a scale, and quota bars
  when `project_quotas` / `project_gpu_quotas` are set. A job is a GPU job when
  Slurm allocated or requested GPUs for it (`AllocTRES`/`ReqTRES` in `sacct`,
  the gres column and the long-format TRES of `squeue`) or when it runs on a
  partition listed in `gpu_partitions`; `gpu_hour_factor` converts Slurm GPU
  units into billed GPU-hours (LUMI-G: 0.5). The first poll of a fresh install
  asks `sacct` for the whole `history_days` window once, so the 30-day figures
  are complete from the start; afterwards finished jobs stay in the local
  history. The ▾ (or `u`) collapses the row to one pill per project.
* **GPUs on your jobs** show as a small tag next to the node count and in the
  job details.
* **Project cards** (mode `pi` only) appear below for every project listed in
  the config: see [Projects](#projects-who-runs-how-much) below. The ▾ at the
  left of the row (or `p`) collapses them to one pill per project with the
  running jobs and the 30-day usage; the choice is remembered.
* Muted teal / coral / mustard palette, dark and light; failed and done never
  rely on a red-green pair. The ◐ button (or `t`) cycles auto / dark / light.
* Keys: `/` search, `r` refresh now, `l` cluster load (again: refetch), `q` back to jobs, `t` theme, `1`-`5` tabs, `Esc` close.

Only the dashboard's own machine can reach it (`listen_host = "127.0.0.1"`).
If you run OmniQueue on a remote machine, forward the port with
`ssh -L 8765:127.0.0.1:8765 thatmachine` rather than opening it up.

## The side widget

`http://127.0.0.1:8765/widget` is a compact page for a narrow window kept at
the side of the screen. `omniqueue monitor widget` starts OmniQueue and opens
just that page, on macOS as a small Safari window docked at the right edge of
the screen (`--plain` gives a normal tab instead); from the dashboard the **▯ widget** button or `w` opens it as
a small pop-up. It shows one line per cluster with running / queued /
failed / done counts and the ssh state, an **Alerts** list of jobs that
failed, timed out, ran out of memory, lost a node or were cancelled in the
last 48 h (dismiss them one by one or all at once), then the most recently
started jobs with elapsed time against their limit and the most recently
finished ones with how long they took.

Press `r` (or the ▶ button) for the **running mode**: every running job with
a progress bar of elapsed time against its limit, the ones closest to their
limit first, turning coral past 90 %. `r` again, `q` or Esc returns to the
summary.

The widget re-reads the server every 10 minutes, and not at all while hidden.
The server polls the clusters as often as the fastest page watching it needs:
with only the widget open that is the widget's 10 minutes, as soon as a
dashboard is open it is `refresh_seconds` again, and with nothing open it
falls back to `refresh_seconds`. Opening a dashboard after a quiet spell
triggers a poll right away. `↻` polls the
clusters right now. Click the bell to allow browser notifications: a new
crashed or timed-out job then pops up a system notification even when the
window is behind others. Query parameters tune it:
`/widget?refresh=300&alerts=24&n=8` re-reads every 5 min, alerts on the last
24 h and lists 8 jobs per section.

## Two modes, two branches

`mode = "user"` shows your own jobs and *your* usage per project. `mode = "pi"`
adds the project-wide parts below: the slow poll of whole projects, their
cards, the project queue view and the experimental predictor. The code is the
same; only the default differs between the two branches of the repository:

| branch | default mode | for |
|---|---|---|
| `USER-version` | `user` | one person watching their own jobs and usage |
| `PI-version` | `pi` | a PI or project manager who also watches whole projects |

Either default can be overridden with `mode = ...` in the config. Fixes land on
the trunk and are merged into both branches; the branches differ by one line
(`DEFAULT_MODE` in `config.py`).

## Projects: who runs how much

Watching whole projects needs `mode = "pi"` (the default on the `PI-version`
branch). List the Slurm accounts you share in a cluster entry (`projects = [...]`) and
OmniQueue watches the *whole* project, every user, on a much slower timescale
than your own jobs: every 2 hours by default (`project_refresh_seconds`,
overridable per cluster). One card per project sits under the cluster cards:

* **running now**: jobs, cores and waiting jobs of the project at the last
  poll, with a bar split by user (hover a segment for the number);
* **last 30 d**: core-hours and jobs of the last 30 (and 7) days, again split
  by user, and a small chart of core-hours per day for the last month;
* **CPU and GPU apart**: on a cluster with GPU partitions the card has two
  pairs of rows. *cpu now / cpu 30 d* count CPU jobs in cores and core-hours,
  *gpu now / gpu 30 d* count GPU jobs in GPUs and GPU-hours, with their own
  per-day chart and a GPU quota bar (`project_gpu_quotas`, or the
  `gres/gpu` group limit from `sshare`). A job is a GPU job when Slurm
  allocated or, while it waits, requested GPUs for it (`AllocTRES` and
  `ReqTRES` in `sacct`; `squeue`'s gres column is used too but does not show
  `--gpus-per-node` requests) or when it runs on a GPU partition. GPU
  partitions are those `sinfo` reports a `gpu` gres for, those where a job
  with GPUs has been seen, and the `gpu_partitions` list in the cluster entry.
  The two sides never mix: a GPU job's cores are not added to the CPU figures.
  Where Slurm's GPU units are not whole GPUs, `gpu_hour_factor` converts them:
  LUMI-G exposes each MI250X as two units and bills half a GPU-hour per unit,
  so `gpu_hour_factor = 0.5` there makes the GPU-hours and the predictor's
  quota check match the invoice. GPUs *in use* stay in Slurm units, since that
  is what `squeue` and the scheduler count.
  A store written before GPUs were tracked is re-fetched once, in the usual
  back-fill chunks, to add the GPU counts;
* the user legend with each person's 30-day core-hours and, where they have
  any, GPU-hours (you are marked);
* a PI name (or any label) next to the project name when `project_pis` maps
  the project to one;
* the small chart of core-hours per day carries a scale: a line at the busiest
  day with its value, and a faint dashed line at half of it;
* the project's **fairshare** factor (and yours), from `sshare`;
* a **quota bar** when a limit is known: `project_quotas` in the config
  (core-hours per rolling 30 days) or, without it, the group limit some sites
  set in Slurm (`GrpTRESMins`, shown as "allocation").

Under the hood one ssh round trip per poll runs `squeue --account=...` for the
project's queue, `sacct --allusers --accounts=...` for its accounting since the
previous poll, `sshare --all --accounts=...` for fairshare and usage, and the
same `sinfo` + all-users `squeue` the load view uses. Everything lands in
`~/.local/share/omniqueue/projects.json`: project jobs are stored individually
and kept for `project_history_days` (90 by default), so the rolling overview
outlives Slurm's own accounting window and a restart never re-fetches history.
Each poll asks `sacct` only for the window from the previous poll minus
`project_overlap_hours` (24) to now, so late accounting records are picked up
and nothing older is transferred twice; `sacct` lists a job whenever it was
active at any moment of that window, so a job that was running at the last
poll and has finished since is updated without any overlap. `sacct` over every
user of a project is slow on a busy accounting database, so the first poll asks
for three days only and the rest of `project_history_days` is back-filled in
`project_backfill_days` chunks one minute apart (the card says "loading history:
24 d so far" meanwhile, and `omniqueue projects --poll` runs the chunks to
completion). One poll may take `project_timeout` seconds (300). Some sites hide
other users' jobs in `sacct`; the card then says so and the per-user split
comes from the queue and from `sshare` only. `omniqueue projects` prints the
same overview in the terminal.

Like everything else, the project poll never opens a connection itself: it
waits for `omniqueue login` and the card says "not logged in" until then. The
↻ button on the card row polls at once.

**Click a card** to see the project's queue as of the last project poll: every
user's running and waiting jobs with name, state, elapsed bar, limit, nodes,
cores, GPUs and partition, your own rows in bold. `q` or Esc returns to your
jobs. The small ↻ on a card and the **↻ refresh queue** button in that view
re-read only the cluster's project `squeue` (running and waiting jobs, plus
their GPU allocations from the long-format `squeue`), which is cheap; the
accounting, fairshare and load samples keep their slow schedule.

### Where to submit? (experimental)

Type `experimental` into the search box and a dashed **where to submit?**
button appears next to *cluster load* (`x` opens it; type the word again to
hide it). Describe a job (nodes, hours, optionally cores and a project) and
`omniqueue.predict` ranks every partition it has data for by an estimated
queue wait, with the reasons spelled out. `omniqueue predict -N 4 -t 12` does
the same in the terminal. The estimate is a transparent heuristic, not a
scheduler simulation:

* **free now**: the share of the last week's load samples in which at least
  the requested nodes were idle, taken as the chance of an immediate start;
* **queue pressure**: nodes asked for by pending jobs relative to the partition
  size, turned into hours with the typical run time of the project's jobs on
  that partition;
* **fairshare**: your Slurm fairshare factor on that project (the project's
  when yours is unknown) scales the queue wait from 0.5x (factor 1) to 1.5x
  (factor 0);
* **nice**: the `nice` value in the cluster entry doubles the estimate per 5000;
* **history**: the median wait of your own similar-sized jobs there in the last
  30 days is blended in when it exists;
* **quota**: a project with fewer core-hours (GPU-hours for a GPU job) left
  than the job needs is left out, one that is nearly used is flagged;
* **GPUs per node** > 0 restricts the candidates to GPU partitions (and
  excludes those with fewer GPUs per node), 0 to CPU partitions;
* partitions whose time limit is too short or that are smaller than the job
  are left out; a candidate whose latest load sample is stale gets "low"
  confidence.

The factors are returned with every candidate so you can compare the estimate
with what really happened and tune the constants at the top of
`src/omniqueue/predict.py`. Ideas for later, in order of usefulness: record
the actual wait of every job you submit and fit the pressure-to-hours
conversion per partition from that; read `sprio` for the priority of the
jobs ahead of you instead of the fairshare proxy; ask `squeue --start` for
Slurm's own backfill estimate of a probe job.

### Building the data with only the widget open

Everything above is gathered by the *server*, not by the page that happens to
be open, so a widget-only setup collects it just as well:

* `omniqueue monitor widget` starts the same Python process as the dashboard;
  the project poll runs in it as a background thread with its own clock
  (`project_refresh_seconds` per cluster), whether a dashboard, a widget or
  nothing at all is looking.
* The widget's 10-minute cadence only governs the *job* poll (the server polls
  at the pace of the fastest page watching). The project poll is independent
  of viewers and never faster than its configured interval, so the extra load
  on a cluster is one `squeue`/`sacct`/`sshare`/`sinfo` round trip every 2 h.
* The poll only runs while you are logged in (`omniqueue login`), so the
  natural rhythm is: log in in the morning, start the widget, and by the
  evening there are six load samples per partition and the day's accounting
  in `projects.json`. Log out; nothing happens until the next login.
* Every poll asks `sacct` only for the time since the previous poll (plus a
  day of slack), and stores jobs by id, so gaps (laptop closed, logged out)
  are filled in on the next poll without duplicates; load samples are simply
  missing for the gap, which the predictor tolerates.
* When you later open the dashboard the cards and the predictor read the
  accumulated store, so the picture is complete even if you never looked at
  it while it was being built. `omniqueue projects` and `omniqueue predict`
  read the same file without any cluster access.
* To let the widget itself show a project line later, it would only need to
  read `/api/projects` on its own slow schedule (the endpoint answers 304 when
  nothing changed); no new cluster traffic would be involved.

## Security notes

OmniQueue runs commands on your HPC accounts, so it is built to do as little
as possible and to fail closed. Before pointing it at real clusters:

* **Use a dedicated key.** Create a key just for OmniQueue
  (`ssh-keygen -t ed25519 -f ~/.ssh/id_omniqueue`) and reference it with
  `ssh_options = ["-i", "~/.ssh/id_omniqueue", "-o", "IdentitiesOnly=yes"]`.
  It only ever needs to run `squeue` and `sacct`; where the centre supports
  it, restrict the key to those commands in `authorized_keys`.
* **Keep the dashboard local.** `listen_host = "127.0.0.1"` is the default and
  the config loader refuses any other address unless you set
  `allow_remote = true` *and* an `access_token` of at least 16 characters.
  With a token, every request must carry it: open
  `http://host:8765/?token=...` once and a cookie is set. To view the dashboard
  from another machine, prefer `ssh -L 8765:127.0.0.1:8765 host` or Tailscale
  over exposing the port.
* **Files are private.** `omniqueue init` writes the config with mode 600,
  the history file is written 600 inside a 700 data directory, and
  `omniqueue check` warns if either has become readable by others. The history
  holds job names, node lists and working directories.
* **`ssh_options` are allow-listed.** Only `-p -i -l -J -o -c -m -b -B -4 -6 -C -q`
  and a fixed set of `-o` keys are accepted. `ProxyCommand`, `LocalCommand`,
  port forwarding, agent/X11 forwarding, `Control*`, `Include` and known_hosts
  overrides are rejected at load time; use `-J`/`ProxyJump` for bastions.
  Every poll also passes `ClearAllForwardings=yes`, `ForwardAgent=no` and
  `ForwardX11=no`, and `squeue_args`/`sacct_args` may not contain shell characters.
* **Host keys are verified.** Unattended polls run with
  `StrictHostKeyChecking=yes`: a host must already be in `~/.ssh/known_hosts`.
  `omniqueue login` connects interactively with `ask`, so you see and confirm a
  new fingerprint yourself. Set `accept_new_host_keys = true` if you prefer
  trust-on-first-use.
* **Persistent connections are optional.** Idle masters close after
  `persist_seconds` (default 4 h). Shorten it, or set
  `persist_connections = false` for a fresh ssh per poll, if a lingering
  authenticated session on your laptop worries you more than the reconnects.
* **The web API is protected.** Every POST (`/api/refresh`, `/api/forget/...`)
  must carry a per-process token that only the served page knows, which stops
  other websites from triggering actions in your browser. Responses carry a
  strict Content-Security-Policy and `nosniff`/`DENY` headers, and no inline
  scripts are used.

## How data is gathered

Per poll and per cluster, OmniQueue runs one remote shell command:

```
squeue --noheader --array --user="$USER" --format='%i|%T|...|%b|%j'; echo "@@OMNIQUEUE squeue rc=$?"; \
squeue --noheader --user="$USER" --Format='JobID:60|,NumNodes:12|,tres-alloc:400|,...'; echo "@@OMNIQUEUE squeue_tres rc=$?"; \
sacct  --noheader --parsable2 --allocations --user="$USER" --starttime=<now - lookback> --format=JobID,State,...,AllocTRES,ReqTRES,JobName; echo "@@OMNIQUEUE sacct rc=$?"
```

The cluster load view, only when you ask for it, runs separately:

```
sinfo  --noheader --format='%P|%a|%D|%T|%C|%l|%z|%c|%G' [--partition=main,gpu]; echo "@@OMNIQUEUE sinfo rc=$?"; \
squeue --noheader --states=RUNNING,PENDING --format='%P|%T|%D|%C' [--partition=main,gpu]; echo "@@OMNIQUEUE squeue_all rc=$?"
```

The project poll, every `project_refresh_seconds` per cluster, runs in one round trip:

```
squeue --noheader --states=RUNNING,PENDING --account=<projects> --format='%i|%a|%u|%T|%P|%D|%C|%l|%M|%b|%j'; \
squeue --noheader --states=RUNNING,PENDING --account=<projects> --Format='JobID:60|,NumNodes:12|,tres-alloc:400|,...'; \
sacct  --noheader --parsable2 --allocations --allusers --accounts=<projects> --starttime=<last poll - 1 d> --format=JobID,Account,User,...,AllocTRES; \
sshare --noheader --parsable2 --all --accounts=<projects> --format=Account,User,RawShares,...,FairShare,GrpTRESMins,GrpTRESRaw,TRESRunMins; \
sinfo ...; squeue --states=RUNNING,PENDING ...          # the same load sample the load view takes
```

`squeue` is authoritative for anything it lists; `sacct` supplies finished jobs
and their exit codes. Everything is merged into a JSON history file in
`~/.local/share/omniqueue/`, so crashed jobs stay visible after the cluster's
accounting window closes. A job that was running and disappears from both
commands is marked `vanished` instead of silently dropping out.

Times are shown exactly as the cluster reports them (cluster local time, no
zone).

## Development

The octopus sprite comes from `tools/make_logo.py` (`logo.svg`, `favicon.svg`).
The lettered logo (`logo_text.svg`, `logo_text_dark.svg` for the dark theme) is
generated by `tools/make_logo_text.py`, which composes the sprite with "Omni /
Queue" in a bold 5x7 pixel font. The hand-drawn smooth-lettered originals are
kept as `logo_text_smooth.svg` and `logo_text_smooth_dark.svg`.

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```
