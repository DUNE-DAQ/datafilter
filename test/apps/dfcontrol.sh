#!/usr/bin/env bash
# dfcontrol.sh -- manage the DataFilter V5 
#
# Usage:
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

# app_key -> binary name, OKS app name
declare -A BIN=( [frw]=filterresultwriter [fo]=filterorchestrator [trd]=trdispatcher [df]=datafilter2 )
declare -A APP=( [frw]=FilterResultWriter_0 [fo]=FilterOrchestrator_0 [trd]=TRDispatcher_0 [df]=DataFilter_0 )
# canonical label for each key (matches binary basename for pkill fallback)
declare -A LABEL=( [frw]=filterresultwriter [fo]=filterorchestrator [trd]=trdispatcher [df]=datafilter )
# start order (FRW/FO bind sockets first; TRD before DF so PUB socket is ready)
START_ORDER=(frw fo trd df)

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

pid_file() { echo "$PID_DIR/${1}.pid"; }

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
    local pf; pf="$(pid_file "$key")"
    if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        echo "  $key already running (PID $(cat "$pf"))"
        return
    fi
    mkdir -p "$LOG_DIR" "$PID_DIR"
    rotate_log "$key"
    local logfile="$LOG_DIR/${key}.log"
    "$BUILD_DIR/${BIN[$key]}" \
        -n "${APP[$key]}" -s "$SESSION" -x "$OKS_CFG" \
        > "$logfile" 2>&1 &
    local pid=$!
    echo "$pid" > "$pf"
    echo "  started ${BIN[$key]} as ${APP[$key]}  PID=$pid  log=$logfile"
}

stop_one() {
    local key="$1"
    local pf; pf="$(pid_file "$key")"
    local pid
    if [ -f "$pf" ]; then
        pid=$(cat "$pf")
    else
        # fallback: find by binary label
        pid=$(pgrep -f "${LABEL[$key]}" 2>/dev/null | head -1)
    fi
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        echo "  ${key} not running"
        rm -f "$pf"
        return
    fi
    echo "  stopping ${key} (PID $pid)..."
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
    rm -f "$pf"
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
        local pid=""
        [ -f "$pf" ] && pid=$(cat "$pf")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "  RUNNING  ${k} (${BIN[$k]})  PID=$pid"
        elif [ -n "$pid" ]; then
            echo "  DEAD     ${k} (${BIN[$k]})  PID=$pid (stale)"
        else
            echo "  STOPPED  ${k} (${BIN[$k]})"
        fi
    done
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

# --- main ---
cmd="${1:-}"
shift || true

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
    stop|kill)    do_stop  "${resolved_keys[@]}" ;;
    restart)      do_stop "${resolved_keys[@]}"; sleep 1; do_start "${resolved_keys[@]}" ;;
    status)       do_status ;;
    monitor)      do_monitor ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|monitor} [app...]"
        echo "  Apps: frw  fo  trd  df  (or full names like trdispatcher)"
        echo "  Env:  DATAFILTER_WORK_DIR  DATAFILTER_BUILD_DIR  DATAFILTER_OUTPUT_DIR"
        exit 1
        ;;
esac
