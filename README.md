# OmniQueue

<img src="src/omniqueue/static/logo.svg" width="96" align="right" alt="">

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
refresh_seconds = 60      # how often every cluster is polled
lookback_hours  = 72      # how far back sacct is asked for finished jobs
history_days    = 30      # finished jobs stay in the local history this long
ssh_timeout     = 20
persist_connections = true   # keep one ssh connection per cluster open between polls
persist_seconds = 28800      # ... for this long after the last poll (8 h)
keepalive_seconds = 15       # notice a dead connection within ~45 s
retry_seconds   = 15         # retry failed clusters after 15 s, 30 s, 60 s ... up to refresh_seconds
listen_host     = "127.0.0.1"
listen_port     = 8765

[[clusters]]
name = "tetralith"
host = "tetralith"          # anything ssh accepts, aliases from ~/.ssh/config included
# user = "x_flotr"          # Slurm user; defaults to $USER on the cluster
# ssh_options = ["-o", "ProxyJump=bastion"]
# squeue_args = ["--partition=main"]
# sacct_args  = ["--account=naiss2024-1-23"]
# use_sacct = false         # for clusters without job accounting
# color = "#5f9e99"         # accent colour in the dashboard
# logo = "~/Pictures/nsc.png" # or drop <name>.svg/.png into ~/.config/omniqueue/logos/

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
(default 8 h) without use. One poll is exactly one ssh session per cluster:
`squeue` and `sacct` run in the same remote shell.

Polls run ssh in batch mode and never prompt. If a cluster needs a password or
a one-time code, open the master by hand first:

```sh
omniqueue login            # all clusters that are not connected yet
omniqueue login dardel     # just one
omniqueue login --close    # tear the connections down
```

You type the password/OTP once; the poller reuses that connection afterwards.
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
| `omniqueue serve [--open]` | the same without opening a browser (for a headless machine) |
| `omniqueue login [CLUSTER...] [--close]` | open (or close) the persistent ssh connection, allowing password / 2FA |
| `omniqueue login --force CLUSTER` | reconnect a cluster whose connection is stale |
| `omniqueue list [--state running] ...` | poll once and print a table to the terminal |
| `omniqueue check` | connect to every cluster once and report problems |
| `omniqueue init [--force]` | write the example config |
| `omniqueue --demo ...` | run any command against fabricated clusters |
| `omniqueue --config PATH ...` | use another config file |

## The dashboard

* **Cluster cards** show running / pending / failed / done counts, the time of
  the last successful poll, and the ssh or Slurm error if the cluster could not
  be reached. Click a card to hide or show that cluster's jobs.
* **Logos**: drop `<cluster name>.svg` (or `.png`, `.jpg`, `.webp`) into
  `~/.config/omniqueue/logos/` and it appears on the card, or set
  `logo = "path-or-https-url"` in the cluster entry. Without one, the card shows
  the cluster's initials in its accent colour.
* Nothing on the page ticks between polls: elapsed times and timestamps are
  those of the last poll, so the layout stays still.
* **Tabs** filter by category. The search box matches loosely: `vsp13`
  finds `vasp-relax-13`, `6195` finds job `619506`. Matched characters are
  highlighted and results are ranked by match quality while you type.
  Several space-separated terms must all match; node list, reason, partition,
  account and work directory are searched too (plain substring).
* **Running jobs** show the elapsed time at the last poll and a bar of the time
  limit used. The bar turns coral past 90 %.
* **Failed jobs** carry a note such as `exit code 1`, `hit time limit`,
  `out of memory` or `cancelled by uid 1234`.
* Click a row for all details (queue wait, node list, work dir, exit code, ...).
  Finished jobs can be removed from the local history from there.
* Muted teal / coral / mustard palette, dark and light; failed and done never
  rely on a red-green pair. The ◐ button (or `t`) cycles auto / dark / light.
* Keys: `/` search, `r` refresh now, `t` theme, `1`-`5` tabs, `Esc` close.

Only the dashboard's own machine can reach it (`listen_host = "127.0.0.1"`).
If you run OmniQueue on a remote machine, forward the port with
`ssh -L 8765:127.0.0.1:8765 thatmachine` rather than opening it up.

## How data is gathered

Per poll and per cluster, OmniQueue runs one remote shell command:

```
squeue --noheader --array --user="$USER" --format='%i|%T|...|%j'; echo "@@OMNIQUEUE squeue rc=$?"; \
sacct  --noheader --parsable2 --allocations --user="$USER" --starttime=<now - lookback> --format=JobID,State,...,JobName; echo "@@OMNIQUEUE sacct rc=$?"
```

`squeue` is authoritative for anything it lists; `sacct` supplies finished jobs
and their exit codes. Everything is merged into a JSON history file in
`~/.local/share/omniqueue/`, so crashed jobs stay visible after the cluster's
accounting window closes. A job that was running and disappears from both
commands is marked `vanished` instead of silently dropping out.

Times are shown exactly as the cluster reports them (cluster local time, no
zone).

## Development

The logo is a 24x24 sprite drawn as ASCII in `tools/make_logo.py`; edit it and
re-run the script to regenerate `logo.svg` and `favicon.svg`.

```sh
python -m unittest discover -s tests -v
```
