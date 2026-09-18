"""Shell completion scripts, printed by ``omniqueue completion <shell>``.

Cluster names are looked up live through the hidden ``omniqueue _clusters``
command, so they follow the config file.
"""

from __future__ import annotations

COMMANDS = {
    "init": "write an example config file",
    "monitor": "start polling and open the dashboard",
    "serve": "start polling without opening a browser",
    "login": "open the persistent ssh connections",
    "active": "show open ssh connections and their remaining time",
    "logout": "close the persistent ssh connections",
    "list": "poll once and print a job table",
    "usage": "your own usage per project (--jobs: how each job was classified)",
    "projects": "who runs how much in your projects",
    "predict": "experimental: where would a job start fastest",
    "check": "test the connection to every cluster",
    "completion": "print a shell completion script",
}
STATES = "running pending ok problem COMPLETED FAILED TIMEOUT OUT_OF_MEMORY CANCELLED NODE_FAIL PREEMPTED"

BASH = r"""# bash completion for omniqueue -- add to ~/.bashrc:  eval "$(omniqueue completion bash)"
_omniqueue() {
  local cur prev cmd="" cfg="" i
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  for ((i = 1; i < COMP_CWORD; i++)); do
    case "${COMP_WORDS[i]}" in
      -c|--config) cfg="--config ${COMP_WORDS[i+1]}"; ((i++)) ;;
      init|monitor|serve|login|active|logout|list|usage|projects|predict|check|completion) cmd="${COMP_WORDS[i]}"; break ;;
    esac
  done
  if [[ "$prev" == "-c" || "$prev" == "--config" ]]; then
    COMPREPLY=( $(compgen -f -- "$cur") ); return
  fi
  if [[ -z "$cmd" ]]; then
    COMPREPLY=( $(compgen -W "__COMMANDS__ --config --demo --verbose --version --help" -- "$cur") ); return
  fi
  case "$cmd" in
    login)  COMPREPLY=( $(compgen -W "--force --close $(omniqueue $cfg _clusters 2>/dev/null)" -- "$cur") ) ;;
    logout) COMPREPLY=( $(compgen -W "$(omniqueue $cfg _clusters 2>/dev/null)" -- "$cur") ) ;;
    list)
      if [[ "$prev" == "-s" || "$prev" == "--state" ]]; then
        COMPREPLY=( $(compgen -W "__STATES__" -- "$cur") )
      else
        COMPREPLY=( $(compgen -W "--state" -- "$cur") )
      fi ;;
    monitor) COMPREPLY=( $(compgen -W "dashboard widget --port --host --no-open --plain" -- "$cur") ) ;;
    projects) COMPREPLY=( $(compgen -W "--poll --partitions" -- "$cur") ) ;;
    usage) COMPREPLY=( $(compgen -W "--jobs $(omniqueue $cfg _clusters 2>/dev/null)" -- "$cur") ) ;;
    predict)
      case "$prev" in
        -M|--cluster) COMPREPLY=( $(compgen -W "$(omniqueue $cfg _clusters 2>/dev/null)" -- "$cur") ) ;;
        *) COMPREPLY=( $(compgen -W "--nodes --hours --cores --gpus --project --cluster --partition" -- "$cur") ) ;;
      esac ;;
    serve)   COMPREPLY=( $(compgen -W "--port --host --open" -- "$cur") ) ;;
    init)    COMPREPLY=( $(compgen -W "--force" -- "$cur") ) ;;
    completion) COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") ) ;;
  esac
}
complete -F _omniqueue omniqueue
"""

ZSH = r"""# zsh completion for omniqueue -- add to ~/.zshrc after compinit:  eval "$(omniqueue completion zsh)"
_omniqueue() {
  local -a cmds
  cmds=(__ZSH_COMMANDS__)
  local context state line
  typeset -A opt_args
  _arguments -C \
    '(-c --config)'{-c,--config}'[config file]:file:_files' \
    '--demo[use fabricated clusters instead of ssh]' \
    '(-v --verbose)'{-v,--verbose}'[debug logging]' \
    '--version[show version]' \
    '1: :->cmd' \
    '*:: :->args'
  case $state in
    cmd) _describe 'command' cmds ;;
    args)
      local -a clusters cfgargs
      [[ -n "${opt_args[--config]}" ]] && cfgargs=(--config "${opt_args[--config]}")
      [[ -n "${opt_args[-c]}" ]] && cfgargs=(--config "${opt_args[-c]}")
      clusters=(${(f)"$(omniqueue $cfgargs _clusters 2>/dev/null)"})
      case $words[1] in
        login)  _arguments '--force[reconnect even if open]' '--close[close instead]' "*:cluster:($clusters)" ;;
        logout) _arguments "*:cluster:($clusters)" ;;
        list)   _arguments '*'{-s,--state}'[only these states]:state:(__STATES__)' ;;
        monitor) _arguments '--port[listen port]:port' '--host[listen host]:host' '--no-open[do not open a browser]' '--plain[widget: normal tab instead of a Safari window]' '1:view:(dashboard widget)' ;;
        projects) _arguments '--poll[poll the clusters now]' '--partitions[per-partition breakdown]' ;;
        usage) _arguments '--jobs[list every stored job with its classification]' "*:cluster:($clusters)" ;;
        predict) _arguments '(-N --nodes)'{-N,--nodes}'[nodes]:nodes' '(-t --hours)'{-t,--hours}'[wall time in hours]:hours' '(-n --cores)'{-n,--cores}'[total cores]:cores' '(-G --gpus)'{-G,--gpus}'[GPUs per node]:gpus' '*'{-A,--project}'[project]:project' '*'{-M,--cluster}"[cluster]:cluster:($clusters)" '*'{-p,--partition}'[partition]:partition' ;;
        serve)   _arguments '--port[listen port]:port' '--host[listen host]:host' '--open[open the dashboard]' ;;
        init)    _arguments '--force[overwrite an existing config]' ;;
        completion) _arguments '1:shell:(bash zsh fish)' ;;
      esac ;;
  esac
}
compdef _omniqueue omniqueue
"""

FISH = r"""# fish completion for omniqueue -- save as ~/.config/fish/completions/omniqueue.fish
#   omniqueue completion fish > ~/.config/fish/completions/omniqueue.fish
complete -c omniqueue -f
complete -c omniqueue -s c -l config -r -F -d 'config file'
complete -c omniqueue -l demo -d 'use fabricated clusters instead of ssh'
complete -c omniqueue -s v -l verbose -d 'debug logging'
complete -c omniqueue -l version -d 'show version'
__FISH_COMMANDS__
complete -c omniqueue -n '__fish_seen_subcommand_from login logout' -a '(omniqueue _clusters 2>/dev/null)' -d cluster
complete -c omniqueue -n '__fish_seen_subcommand_from login' -l force -d 'reconnect even if open'
complete -c omniqueue -n '__fish_seen_subcommand_from login' -l close -d 'close instead'
complete -c omniqueue -n '__fish_seen_subcommand_from list' -s s -l state -x -a '__STATES__' -d 'only these states'
complete -c omniqueue -n '__fish_seen_subcommand_from monitor serve' -l port -x -d 'listen port'
complete -c omniqueue -n '__fish_seen_subcommand_from monitor serve' -l host -x -d 'listen host'
complete -c omniqueue -n '__fish_seen_subcommand_from monitor' -l no-open -d 'do not open a browser'
complete -c omniqueue -n '__fish_seen_subcommand_from monitor' -a 'dashboard widget' -d 'what to open'
complete -c omniqueue -n '__fish_seen_subcommand_from monitor' -l plain -d 'widget: normal tab instead of a Safari window'
complete -c omniqueue -n '__fish_seen_subcommand_from serve' -l open -d 'open the dashboard'
complete -c omniqueue -n '__fish_seen_subcommand_from projects' -l poll -d 'poll the clusters now'
complete -c omniqueue -n '__fish_seen_subcommand_from projects' -l partitions -d 'per-partition breakdown'
complete -c omniqueue -n '__fish_seen_subcommand_from usage' -l jobs -d 'list every stored job with its classification'
complete -c omniqueue -n '__fish_seen_subcommand_from usage' -a '(omniqueue _clusters 2>/dev/null)' -d cluster
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s N -l nodes -x -d 'nodes'
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s t -l hours -x -d 'wall time in hours'
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s n -l cores -x -d 'total cores'
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s G -l gpus -x -d 'GPUs per node'
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s A -l project -x -d 'project'
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s M -l cluster -x -a '(omniqueue _clusters 2>/dev/null)' -d 'cluster'
complete -c omniqueue -n '__fish_seen_subcommand_from predict' -s p -l partition -x -d 'partition'
complete -c omniqueue -n '__fish_seen_subcommand_from init' -l force -d 'overwrite an existing config'
complete -c omniqueue -n '__fish_seen_subcommand_from completion' -a 'bash zsh fish'
"""


def script(shell: str) -> str:
    if shell == "bash":
        return BASH.replace("__COMMANDS__", " ".join(COMMANDS)).replace("__STATES__", STATES)
    if shell == "zsh":
        cmds = " ".join(f"'{name}:{desc}'" for name, desc in COMMANDS.items())
        return ZSH.replace("__ZSH_COMMANDS__", cmds).replace("__STATES__", STATES)
    if shell == "fish":
        cmds = "\n".join(
            f"complete -c omniqueue -n '__fish_use_subcommand' -a {name} -d '{desc}'" for name, desc in COMMANDS.items()
        )
        return FISH.replace("__FISH_COMMANDS__", cmds).replace("__STATES__", STATES)
    raise ValueError(f"unsupported shell {shell!r} (bash, zsh or fish)")
