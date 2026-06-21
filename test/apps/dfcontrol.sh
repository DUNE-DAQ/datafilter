#!/usr/bin/env bash
# dfcontrol.sh -- manage the DataFilter V5 
#
# Usage:
#   dfcontrol.sh build              -- build all four apps
#   dfcontrol.sh start              -- start all four apps
#   dfcontrol.sh stop               -- graceful stop all apps (SIGTERM, fallback SIGKILL)
#   dfcontrol.sh restart            -- stop then start all apps
#   dfcontrol.sh status             -- show running/stopped state of each app
#   dfcontrol.sh stop  trdispatcher -- stop only TRD (for restart with new files)
#   dfcontrol.sh start trdispatcher -- start only TRD
#   dfcontrol.sh monitor            -- tmux view: HDF5 output dir + 4 log tails
#
# The working directory (where hdf5_files_list.json lives) is resolved in order:
#   1. DATAFILTER_WORK_DIR environment variable
#   2. Default hardcoded path (if it exists)
#   3. Current directory (pwd)
#
# BUILD_DIR is derived from WORK_DIR unless DATAFILTER_BUILD_DIR is set.

DEFAULT_WORK_DIR="/lcg/storage19/test-area/fddaq-v5-work_dir"
if [ -n "${DATAFILTER_WORK_DIR:-}" ]; then
    WORK_DIR="$DATAFILTER_WORK_DIR"
elif [ -d "$DEFAULT_WORK_DIR" ]; then
    WORK_DIR="$DEFAULT_WORK_DIR"
else
    WORK_DIR="$(pwd)"
    echo "Note: using current directory as WORK_DIR: $WORK_DIR"
fi

BUILD_DIR="${DATAFILTER_BUILD_DIR:-$WORK_DIR/build/datafilter/apps}"
OKS_CFG="oksconflibs:test/config/dfSession.data.xml"
SESSION="test-session"
OUTPUT_DIR="${DATAFILTER_OUTPUT_DIR:-/lcg/storage18/dune/chen}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="/tmp/datafilter-pids"

# --- supervise defaults (overridable via supervise CLI flags) ---
SUPERVISE_INTERVAL=2
SUPERVISE_MAX_RESTARTS=3
SUPERVISE_WINDOW=60
SUPERVISE_BACKOFF_MAX=16
SUPERVISE_HEALTHY_AFTER=30
SUPERVISE_LOG="$LOG_DIR/supervise.log"

# supervisor in-memory state (populated by cmd_supervise)
declare -A SUP_WATCH        # key -> 1 if opted-in for this run
declare -A SUP_FAILED       # key -> 1 if max-restarts hit, no longer watched
declare -A SUP_HISTORY      # key -> space-separated epoch timestamps of restarts
declare -A SUP_BACKOFF      # key -> next backoff seconds (1,2,4,...)
declare -A SUP_LAST_START   # key -> epoch of last start_one call
declare -A SUP_LAST_STATE   # key -> last printed state, to debounce status lines

# app_key -> binary name, OKS app name
declare -A BIN=( [frw]=filterresultwriter [fo]=filterorchestrator [trd]=trdispatcher [df]=datafilter2 )
declare -A APP=( [frw]=FilterResultWriter_0 [fo]=FilterOrchestrator_0 [trd]=TRDispatcher_0 [df]=DataFilter_0 )
# canonical label for each key (matches binary basename for pkill fallback)
declare -A LABEL=( [frw]=filterresultwriter [fo]=filterorchestrator [trd]=trdispatcher [df]=datafilter )
# start order (FRW/FO bind sockets first; TRD before DF so PUB socket is ready)
START_ORDER=(frw fo trd df)

# NetworkConnection id whose address (tcp://HOST:PORT) gives each app's host
declare -A CONN_ID=( [frw]=trwriter0 [fo]=FO_ctrl0 [trd]=trdispatcher0 [df]=conn_A1_G0_C0_ )
OKS_DATA_XML="$SCRIPT_DIR/../config/dfSession.data.xml"

# Use supervisord if both supervisord and supervisorctl are available
USE_SUPERVISORD=0
if command -v supervisord &>/dev/null && command -v supervisorctl &>/dev/null; then
    USE_SUPERVISORD=1
fi
SUPERVISORD_CONF="$SCRIPT_DIR/supervisord.conf"
SUPERVISORD_SOCK="/tmp/datafilter-supervisor.sock"
SUPERVISORD_PID="/tmp/datafilter-supervisord.pid"
SUPERVISE_PID_FILE="/tmp/datafilter-supervise.pid"

# Map user-friendly name (trdispatcher, df, datafilter, ...) to internal key
resolve_key() {
    case "$1" in
        frw|filterresultwriter|FilterResultWriter_0) echo frw ;;
        fo|filterorchestrator|FilterOrchestrator_0)  echo fo  ;;
        trd|trdispatcher|TRDispatcher_0)             echo trd ;;
        df|datafilter|datafilter2|DataFilter_0)      echo df  ;;
        *) echo ""; return 1 ;;
    esac
}

pid_file()  { echo "$PID_DIR/${1}.pid"; }
host_file() { echo "$PID_DIR/${1}.host"; }

_is_local() {
    local h="$1"
    [ -z "$h" ] || [ "$h" = "localhost" ] || [ "$h" = "127.0.0.1" ] || \
    [ "$h" = "$(hostname -s 2>/dev/null)" ] || [ "$h" = "$(hostname -f 2>/dev/null)" ]
}

_remote_alive() {
    local host="$1" pid="$2"
    [ -n "$pid" ] || return 1
    if _is_local "$host"; then
        kill -0 "$pid" 2>/dev/null
    else
        ssh "$host" "kill -0 $pid" 2>/dev/null
    fi
}

# Extract host from tcp://HOST:PORT address of the app's canonical NetworkConnection.
get_app_host() {
    local key="$1"
    python3 -c "
import xml.etree.ElementTree as ET, re, sys
root = ET.parse('$OKS_DATA_XML').getroot()
cid = '${CONN_ID[$key]}'
for obj in root.findall(\"obj[@class='NetworkConnection'][@id='\" + cid + \"']\"):
    a = obj.find(\"attr[@name='address']\")
    if a is not None:
        m = re.match(r'tcp://([^:]+):', a.get('val', ''))
        if m: print(m.group(1)); sys.exit(0)
print('localhost')
" 2>/dev/null || echo "localhost"
}

# max log archives to keep per app
declare -A LOG_KEEP=( [frw]=20 [df]=20 [trd]=5 [fo]=5 )

# rotate_log KEY -- archive existing log with timestamp, trim to LOG_KEEP[KEY]
rotate_log() {
    local key="$1"
    local logfile="$LOG_DIR/${key}.log"
    if [ -f "$logfile" ]; then
        local ts; ts="$(date +%Y%m%d_%H%M%S)"
        mv "$logfile" "$LOG_DIR/${key}_${ts}.log"
        local keep="${LOG_KEEP[$key]:-20}"
        ls -1t "$LOG_DIR/${key}_"*.log 2>/dev/null | tail -n +"$(( keep + 1 ))" | xargs -r rm -f
    fi
}

start_one() {
    local key="$1"
    local host; host=$(get_app_host "$key")
    local pf; pf="$(pid_file "$key")"
    local hf; hf="$(host_file "$key")"
    if [ -f "$pf" ] && _remote_alive "$host" "$(cat "$pf" 2>/dev/null)"; then
        echo "  $key already running (PID $(cat "$pf") on ${host})"
        return
    fi
    mkdir -p "$LOG_DIR" "$PID_DIR"
    rotate_log "$key"
    local logfile="$LOG_DIR/${key}.log"
    local cmd="\"$BUILD_DIR/${BIN[$key]}\" -n \"${APP[$key]}\" -s \"$SESSION\" -x \"$OKS_CFG\""
    local pid
    if _is_local "$host"; then
        eval "$cmd > \"$logfile\" 2>&1 &"
        pid=$!
    else
        pid=$(ssh "$host" "bash -c '$cmd > \"$logfile\" 2>&1 & echo \$!'")
    fi
    echo "$pid" > "$pf"
    echo "$host" > "$hf"
    echo "  started ${BIN[$key]} as ${APP[$key]} on ${host}  PID=$pid  log=$logfile"
}

stop_one() {
    local key="$1"
    local pf; pf="$(pid_file "$key")"
    local hf; hf="$(host_file "$key")"
    local host="" pid=""
    [ -f "$hf" ] && host=$(cat "$hf")
    [ -f "$pf" ] && pid=$(cat "$pf")
    if [ -z "$pid" ] || ! _remote_alive "$host" "$pid"; then
        echo "  ${key} not running"
        rm -f "$pf" "$hf"
        return
    fi
    echo "  stopping ${key} (PID $pid on ${host:-localhost})..."
    if _is_local "$host"; then
        kill -TERM "$pid"
        local deadline=$(( $(date +%s) + 10 ))
        while kill -0 "$pid" 2>/dev/null; do
            if [ "$(date +%s)" -ge "$deadline" ]; then
                echo "  timeout -- force-killing ${key} (PID $pid)"
                kill -9 "$pid" 2>/dev/null
                break
            fi
            sleep 0.3
        done
    else
        ssh "$host" "kill -TERM $pid; sleep 5; kill -0 $pid 2>/dev/null && kill -9 $pid 2>/dev/null; true"
    fi
    rm -f "$pf" "$hf"
    echo "  ${key} stopped"
}

do_start() {
    local keys=("$@")
    [ ${#keys[@]} -eq 0 ] && keys=("${START_ORDER[@]}")
    cd "$SCRIPT_DIR" || { echo "Cannot cd to $SCRIPT_DIR"; exit 1; }
    for k in "${keys[@]}"; do
        start_one "$k"
        # brief pause between apps so sockets bind before the next connects
        sleep 0.4
    done
}

do_stop() {
    local keys=("$@")
    if [ ${#keys[@]} -eq 0 ]; then
        # stop in reverse order
        for (( i=${#START_ORDER[@]}-1; i>=0; i-- )); do
            stop_one "${START_ORDER[$i]}"
        done
    else
        for k in "${keys[@]}"; do stop_one "$k"; done
    fi
}

do_status() {
    for k in "${START_ORDER[@]}"; do
        local pf; pf="$(pid_file "$k")"
        local hf; hf="$(host_file "$k")"
        local pid="" host=""
        [ -f "$pf" ] && pid=$(cat "$pf")
        [ -f "$hf" ] && host=$(cat "$hf")
        host="${host:-localhost}"
        if [ -n "$pid" ] && _remote_alive "$host" "$pid"; then
            echo "  RUNNING  ${k} (${BIN[$k]})  PID=$pid  host=$host"
        elif [ -n "$pid" ]; then
            echo "  DEAD     ${k} (${BIN[$k]})  PID=$pid  host=$host (stale)"
        else
            echo "  STOPPED  ${k} (${BIN[$k]})"
        fi
    done
}

do_build(){
    dbt-build
}
do_monitor() {
    if ! command -v tmux &>/dev/null; then
        echo "tmux not found -- tailing all logs (interleaved):"
        tail -f "$LOG_DIR"/trd.log "$LOG_DIR"/fo.log \
                 "$LOG_DIR"/frw.log "$LOG_DIR"/df.log 2>/dev/null
        return
    fi

    local sn="datafilter-monitor"
    tmux kill-session -t "$sn" 2>/dev/null

    # tail lines per pane -- drives both the tail -n count and the pane height
    local n_watch=8 n_df=50 n_frw=50 n_fo=10 n_trd=10
    local total=$(( n_watch + n_df + n_frw + n_fo + n_trd ))
    local avail=$(( $(tput lines) - 6 ))   # subtract tmux borders/status

    # single-column layout, panes 0-4 top-to-bottom: watch, df, frw, fo, trd
    tmux new-session  -d -s "$sn" -x "$(tput cols)" -y "$(tput lines)" \
        "watch -n 2 'echo \"=== HDF5 output: $OUTPUT_DIR ===\"; ls -lth \"$OUTPUT_DIR\" 2>/dev/null | head -5'"
    tmux split-window -v -t "$sn:0.0" "tail -n $n_df  -f \"$LOG_DIR/df.log\"  2>/dev/null || { echo 'df.log not found';  sleep 9999; }"
    tmux split-window -v -t "$sn:0.1" "tail -n $n_frw -f \"$LOG_DIR/frw.log\" 2>/dev/null || { echo 'frw.log not found'; sleep 9999; }"
    tmux split-window -v -t "$sn:0.2" "tail -n $n_fo  -f \"$LOG_DIR/fo.log\"  2>/dev/null || { echo 'fo.log not found';  sleep 9999; }"
    tmux split-window -v -t "$sn:0.3" "tail -n $n_trd -f \"$LOG_DIR/trd.log\" 2>/dev/null || { echo 'trd.log not found'; sleep 9999; }"

    # resize proportionally (last pane gets the remainder)
    tmux resize-pane -t "$sn:0.0" -y $(( avail * n_watch / total ))
    tmux resize-pane -t "$sn:0.1" -y $(( avail * n_df    / total ))
    tmux resize-pane -t "$sn:0.2" -y $(( avail * n_frw   / total ))
    tmux resize-pane -t "$sn:0.3" -y $(( avail * n_fo    / total ))

    tmux select-pane -t "$sn:0.0"
    tmux attach-session -t "$sn"
}

# --- supervisord integration ---

generate_supervisord_conf() {
    mkdir -p "$LOG_DIR"
    cat > "$SUPERVISORD_CONF" <<EOF
[unix_http_server]
file=$SUPERVISORD_SOCK

[supervisord]
logfile=$LOG_DIR/supervisord.log
pidfile=$SUPERVISORD_PID
nodaemon=false

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix://$SUPERVISORD_SOCK

[program:frw]
command=$BUILD_DIR/${BIN[frw]} -n ${APP[frw]} -s $SESSION -x $OKS_CFG
directory=$SCRIPT_DIR
autostart=false
autorestart=true
priority=10
stdout_logfile=$LOG_DIR/frw.log
redirect_stderr=true

[program:fo]
command=$BUILD_DIR/${BIN[fo]} -n ${APP[fo]} -s $SESSION -x $OKS_CFG
directory=$SCRIPT_DIR
autostart=false
autorestart=true
priority=20
stdout_logfile=$LOG_DIR/fo.log
redirect_stderr=true

[program:trd]
command=$BUILD_DIR/${BIN[trd]} -n ${APP[trd]} -s $SESSION -x $OKS_CFG
directory=$SCRIPT_DIR
autostart=false
autorestart=true
priority=30
stdout_logfile=$LOG_DIR/trd.log
redirect_stderr=true

[program:df]
command=$BUILD_DIR/${BIN[df]} -n ${APP[df]} -s $SESSION -x $OKS_CFG
directory=$SCRIPT_DIR
autostart=false
autorestart=true
priority=40
stdout_logfile=$LOG_DIR/df.log
redirect_stderr=true
EOF
    echo "supervisord config written to $SUPERVISORD_CONF"
}

_supervisord_running() {
    [ -f "$SUPERVISORD_PID" ] && kill -0 "$(cat "$SUPERVISORD_PID")" 2>/dev/null
}

do_start_supervisord() {
    cd "$SCRIPT_DIR" || { echo "Cannot cd to $SCRIPT_DIR"; exit 1; }
    generate_supervisord_conf
    if ! _supervisord_running; then
        supervisord -c "$SUPERVISORD_CONF"
        sleep 1
    fi
    for key in "${START_ORDER[@]}"; do
        supervisorctl -c "$SUPERVISORD_CONF" start "$key"
        sleep 0.4
    done
}

do_stop_supervisord() {
    if ! _supervisord_running; then
        echo "supervisord is not running"
        return
    fi
    supervisorctl -c "$SUPERVISORD_CONF" stop all
    supervisorctl -c "$SUPERVISORD_CONF" shutdown
}

do_status_supervisord() {
    if ! _supervisord_running; then
        echo "supervisord is not running"
        return
    fi
    for k in "${START_ORDER[@]}"; do
        local hf; hf="$(host_file "$k")"
        local host="localhost"
        [ -f "$hf" ] && host=$(cat "$hf")
        local state
        state=$(supervisorctl -c "$SUPERVISORD_CONF" status "$k" 2>/dev/null | awk '{print $2}')
        printf "  %-8s %s (%s)  host=%s\n" "${state:-UNKNOWN}" "$k" "${BIN[$k]}" "$host"
    done
}

# --- supervise: auto-restart crashed apps ---

# timestamped log line, to stdout and supervise.log
sup_log() {
    local msg="$1"
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg" | tee -a "$SUPERVISE_LOG"
}

# archive previous supervise.log on each supervise invocation
rotate_supervise_log() {
    if [ -f "$SUPERVISE_LOG" ]; then
        local ts; ts="$(date +%Y%m%d_%H%M%S)"
        mv "$SUPERVISE_LOG" "$LOG_DIR/supervise_${ts}.log"
    fi
}

# parse --key=value flags for supervise; defaults watch every app in START_ORDER
parse_supervise_args() {
    local arg val k
    while [ $# -gt 0 ]; do
        arg="$1"
        case "$arg" in
            --apps=*)
                val="${arg#--apps=}"
                IFS=',' read -r -a _sup_apps <<< "$val"
                for k in "${_sup_apps[@]}"; do
                    local rk; rk="$(resolve_key "$k")"
                    if [ -z "$rk" ]; then
                        echo "supervise: unknown app '$k'" >&2; exit 1
                    fi
                    SUP_WATCH[$rk]=1
                done
                ;;
            --interval=*)       SUPERVISE_INTERVAL="${arg#--interval=}" ;;
            --max-restarts=*)   SUPERVISE_MAX_RESTARTS="${arg#--max-restarts=}" ;;
            --window=*)         SUPERVISE_WINDOW="${arg#--window=}" ;;
            --backoff-max=*)    SUPERVISE_BACKOFF_MAX="${arg#--backoff-max=}" ;;
            --healthy-after=*)  SUPERVISE_HEALTHY_AFTER="${arg#--healthy-after=}" ;;
            *)
                echo "supervise: unknown flag '$arg'" >&2
                exit 1
                ;;
        esac
        shift
    done
    # default: watch every app
    if [ ${#SUP_WATCH[@]} -eq 0 ]; then
        for k in "${START_ORDER[@]}"; do SUP_WATCH[$k]=1; done
    fi
}

# drop timestamps older than NOW - SUPERVISE_WINDOW from SUP_HISTORY[key]
prune_restart_history() {
    local key="$1" now="$2" cutoff t kept=""
    cutoff=$(( now - SUPERVISE_WINDOW ))
    for t in ${SUP_HISTORY[$key]:-}; do
        if [ "$t" -ge "$cutoff" ]; then
            kept+=" $t"
        fi
    done
    SUP_HISTORY[$key]="${kept# }"
}

restart_count() {
    local key="$1"
    # shellcheck disable=SC2086
    set -- ${SUP_HISTORY[$key]:-}
    echo $#
}

# 0=alive, 1=PID file present but process dead, 2=PID file absent (user-stopped)
app_pid_alive() {
    local key="$1" pf pid
    pf="$(pid_file "$key")"; hf="$(host_file "$key")"
    [ -f "$pf" ] || return 2
    pid=$(cat "$pf" 2>/dev/null)
    host=""; [ -f "$hf" ] && host=$(cat "$hf")
    [ -n "$pid" ] && _remote_alive "$host" "$pid" && return 0
    return 1
}

should_restart_app() {
    local key="$1" now="$2"
    [ "${SUP_WATCH[$key]:-0}" = "1" ] || return 1
    [ -z "${SUP_FAILED[$key]:-}" ] || return 1
    app_pid_alive "$key"
    [ $? -eq 1 ] || return 1
    local last="${SUP_LAST_START[$key]:-0}"
    local cooldown="${SUP_BACKOFF[$key]:-1}"
    [ $(( now - last )) -ge "$cooldown" ] || return 1
    return 0
}

# reset backoff/history if the app has been up for SUPERVISE_HEALTHY_AFTER seconds
supervise_check_healthy() {
    local key="$1" now="$2"
    app_pid_alive "$key"; [ $? -eq 0 ] || return 0
    local last="${SUP_LAST_START[$key]:-0}"
    [ "$last" -gt 0 ] || return 0
    if [ $(( now - last )) -ge "$SUPERVISE_HEALTHY_AFTER" ] \
       && [ "${SUP_BACKOFF[$key]:-1}" -ne 1 -o -n "${SUP_HISTORY[$key]:-}" ]; then
        SUP_BACKOFF[$key]=1
        SUP_HISTORY[$key]=""
        sup_log "$key: healthy for ${SUPERVISE_HEALTHY_AFTER}s, backoff reset"
    fi
}

mark_app_failed() {
    local key="$1"
    SUP_FAILED[$key]=1
    sup_log "FAILED $key: exceeded $SUPERVISE_MAX_RESTARTS restarts in ${SUPERVISE_WINDOW}s; no longer watching"
}

supervise_restart_one() {
    local key="$1" now="$2"
    prune_restart_history "$key" "$now"
    local n; n=$(restart_count "$key")
    if [ "$n" -ge "$SUPERVISE_MAX_RESTARTS" ]; then
        mark_app_failed "$key"
        return
    fi
    local bk="${SUP_BACKOFF[$key]:-1}"
    SUP_HISTORY[$key]="${SUP_HISTORY[$key]:-} $now"
    sup_log "restart attempt $(( n + 1 ))/$SUPERVISE_MAX_RESTARTS for $key (backoff was ${bk}s)"
    start_one "$key"
    SUP_LAST_START[$key]="$now"
    local next=$(( bk * 2 ))
    [ "$next" -gt "$SUPERVISE_BACKOFF_MAX" ] && next="$SUPERVISE_BACKOFF_MAX"
    SUP_BACKOFF[$key]="$next"
}

# one pass over all watched apps
supervise_one_cycle() {
    local now="$1" k state rc
    for k in "${START_ORDER[@]}"; do
        [ "${SUP_WATCH[$k]:-0}" = "1" ] || continue
        [ -z "${SUP_FAILED[$k]:-}" ] || continue
        app_pid_alive "$k"; rc=$?
        case "$rc" in
            0) state="running" ;;
            1) state="dead"    ;;
            2) state="stopped" ;;
        esac
        if [ "${SUP_LAST_STATE[$k]:-}" != "$state" ]; then
            sup_log "$k: $state"
            SUP_LAST_STATE[$k]="$state"
        fi
        supervise_check_healthy "$k" "$now"
        if should_restart_app "$k" "$now"; then
            supervise_restart_one "$k" "$now"
        fi
    done
}

_cmd_supervise_inhouse() {
    parse_supervise_args "$@"
    mkdir -p "$LOG_DIR" "$PID_DIR"
    cd "$SCRIPT_DIR" || { echo "Cannot cd to $SCRIPT_DIR"; exit 1; }
    rotate_supervise_log
    echo "$$" > "$SUPERVISE_PID_FILE"
    local k
    for k in "${!SUP_WATCH[@]}"; do
        SUP_BACKOFF[$k]=1
        SUP_HISTORY[$k]=""
        SUP_LAST_START[$k]=0
        SUP_LAST_STATE[$k]=""
    done
    trap 'sup_log "supervisor exiting (apps left running)"; rm -f "$SUPERVISE_PID_FILE"; exit 0' INT TERM
    sup_log "supervisor started; watching=${!SUP_WATCH[*]} interval=${SUPERVISE_INTERVAL}s max=${SUPERVISE_MAX_RESTARTS}/${SUPERVISE_WINDOW}s backoff_cap=${SUPERVISE_BACKOFF_MAX}s healthy_after=${SUPERVISE_HEALTHY_AFTER}s"
    while true; do
        supervise_one_cycle "$(date +%s)"
        sleep "$SUPERVISE_INTERVAL"
    done
}

do_stop_supervise() {
    if _supervisord_running; then
        supervisorctl -c "$SUPERVISORD_CONF" stop all
        supervisorctl -c "$SUPERVISORD_CONF" shutdown
        echo "supervisord stopped"
    elif [ -f "$SUPERVISE_PID_FILE" ]; then
        local pid; pid=$(cat "$SUPERVISE_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid"
            echo "supervise watchdog (PID $pid) stopped"
        else
            echo "supervise watchdog not running (stale PID file)"
        fi
        rm -f "$SUPERVISE_PID_FILE"
    else
        echo "no supervise watchdog running"
    fi
}

cmd_supervise() {
    if [ "$USE_SUPERVISORD" -eq 1 ]; then
        cd "$SCRIPT_DIR" || { echo "Cannot cd to $SCRIPT_DIR"; exit 1; }
        generate_supervisord_conf
        if ! _supervisord_running; then
            supervisord -c "$SUPERVISORD_CONF"
            sleep 1   # wait for daemon socket to be ready
        fi
        for key in "${START_ORDER[@]}"; do
            local host; host=$(get_app_host "$key")
            stop_one "$key"   # hand over: stop direct instance if running
            supervisorctl -c "$SUPERVISORD_CONF" start "$key"
            echo "$host" > "$(host_file "$key")"
            sleep 0.4
        done
        echo "supervisord managing apps. Use: supervisorctl -c $SUPERVISORD_CONF status"
    else
        _cmd_supervise_inhouse "$@"
    fi
}

# --- main ---
cmd="${1:-}"
shift || true

# supervise uses --key=value flags, not bare app names; bypass the resolve loop
if [ "$cmd" = "supervise" ]; then
    if [ "${1:-}" = "stop" ]; then
        do_stop_supervise
    else
        cmd_supervise "$@"
    fi
    exit 0
fi

# Resolve any extra args to internal keys
resolved_keys=()
for arg in "$@"; do
    k=$(resolve_key "$arg")
    if [ -z "$k" ]; then
        echo "Unknown app '$arg'. Valid: frw fo trd df (or full names)."
        exit 1
    fi
    resolved_keys+=("$k")
done

case "$cmd" in
    start)        do_start "${resolved_keys[@]}" ;;
    stop|kill)
        if _supervisord_running; then do_stop_supervisord
        else do_stop "${resolved_keys[@]}"; fi ;;
    restart)
        if _supervisord_running; then
            supervisorctl -c "$SUPERVISORD_CONF" restart all
        else do_stop "${resolved_keys[@]}"; sleep 1; do_start "${resolved_keys[@]}"; fi ;;
    status)
        if _supervisord_running; then do_status_supervisord
        else do_status; fi ;;
    build)        do_build ;;
    monitor)      do_monitor ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|monitor|supervise} [app...]"
        echo "  Apps: frw  fo  trd  df  (or full names like trdispatcher)"
        echo "  Env:  DATAFILTER_WORK_DIR  DATAFILTER_BUILD_DIR  DATAFILTER_OUTPUT_DIR"
        echo
        echo "  supervise [--apps=df,frw,trd,fo] [--interval=2] [--max-restarts=3] \\"
        echo "            [--window=60] [--backoff-max=16] [--healthy-after=30]"
        echo "    Watchdog: restarts apps that crashed (PID file present, process dead)."
        echo "    Uses supervisord if available, else in-house foreground loop."
        echo "    Ctrl+C or 'dfcontrol.sh supervise stop' exits the watchdog."
        echo "  supervise stop"
        echo "    Stop the running watchdog (supervisord or in-house)."
        exit 1
        ;;
esac
