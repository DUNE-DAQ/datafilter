# setup_influx.sh -- export the INFLUXDB_* variables df_to_influx.py needs.
#
# Must be SOURCED, not executed, so the exports persist in your shell:
#   source setup_influx.sh
#   . setup_influx.sh
#
# Edit the placeholders below directly, or (recommended, keeps secrets out of git)
# copy setup_influx.local.sh.example to setup_influx.local.sh in this same directory
# and fill in real values there -- it's gitignored and sourced automatically if present.

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    echo "ERROR: setup_influx.sh must be sourced, not executed:" >&2
    echo "  source setup_influx.sh" >&2
    exit 1
fi

export INFLUXDB_URL="${INFLUXDB_URL:-http://10.0.0.59:8086}"
export INFLUXDB_TOKEN="${INFLUXDB_TOKEN:-koF1-2dN-gzDuGJdmOU3aZqBzXvQzGjQE3RDSdDZ7HKsIdqNbg2M9BcmFehEnlPP-HXGmKdcPpMZZjbdrmR17g==}"
export INFLUXDB_ORG="${INFLUXDB_ORG:-UdeM}"
export INFLUXDB_BUCKET="${INFLUXDB_BUCKET:-dunedaq}"

_setup_influx_local="$(dirname "${BASH_SOURCE[0]}")/setup_influx.local.sh"
if [ -f "$_setup_influx_local" ]; then
    source "$_setup_influx_local"
fi
unset _setup_influx_local

_setup_influx_missing=()
[ -z "$INFLUXDB_TOKEN" ]  && _setup_influx_missing+=("INFLUXDB_TOKEN")
[ -z "$INFLUXDB_ORG" ]    && _setup_influx_missing+=("INFLUXDB_ORG")
[ -z "$INFLUXDB_BUCKET" ] && _setup_influx_missing+=("INFLUXDB_BUCKET")

if [ ${#_setup_influx_missing[@]} -gt 0 ]; then
    echo "setup_influx.sh: WARNING -- not set: ${_setup_influx_missing[*]}" >&2
    echo "  Edit setup_influx.sh directly, or create setup_influx.local.sh" >&2
    echo "  (gitignored, see setup_influx.local.sh.example) with the missing exports." >&2
else
    echo "setup_influx.sh: INFLUXDB_URL=$INFLUXDB_URL INFLUXDB_ORG=$INFLUXDB_ORG INFLUXDB_BUCKET=$INFLUXDB_BUCKET (token set)"
fi
unset _setup_influx_missing
