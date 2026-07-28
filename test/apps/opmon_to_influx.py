#!/usr/bin/env python3
"""
Forward DataFilter's accept/reject ADC histograms from the opmon file sink
(./info.json, per the session's OpMonURI config in dfSession.data.xml) to
InfluxDB 2.x/3.x.

The exact JSON envelope opmonlib writes wasn't verifiable when this script was
written (tool outage mid-session), so records are searched recursively for the
accepted_adc_histogram/rejected_adc_histogram fields rather than assuming a
fixed shape -- this also naturally filters out the other three apps' opmon
records, since only DataFilterInfo carries these fields.

Usage:
  INFLUXDB_URL=https://host:8086 INFLUXDB_TOKEN=... INFLUXDB_ORG=... \\
      INFLUXDB_BUCKET=... python3 opmon_to_influx.py [--file info.json]
"""

import argparse
import json
import os
import sys
import time
import xml.etree.ElementTree as ET

# influxdb_client is imported lazily (inside main()/histogram_points()) so that
# --check-enabled -- used by dfcontrol.sh purely to read a boolean from OKS
# config -- works even when the influxdb-client pip package isn't installed.

HIST_FIELDS = ("accepted_adc_histogram", "rejected_adc_histogram")


def read_oks_attrs(oks_file, obj_class, obj_id):
    """Parse a subset of OKS-XML: find <obj class=obj_class id=obj_id> at the
    top level of oks_file and return {attr_name: val} for its direct <attr
    name=... val=.../> children. Does not follow <include>s or inheritance --
    the object must be defined directly in this file (true for DataFilter_0
    in dfSession.data.xml). Returns None if the object isn't found."""
    root = ET.parse(oks_file).getroot()
    for obj in root.findall("obj"):
        if obj.get("class") == obj_class and obj.get("id") == obj_id:
            return {attr.get("name"): attr.get("val")
                    for attr in obj.findall("attr")
                    if attr.get("val") is not None}
    return None


def oks_bool(val):
    return str(val).strip().lower() not in ("0", "false", "", "none")


def find_histogram_records(obj, path=()):
    """Recursively yield (path, dict) for any dict containing a histogram field."""
    if isinstance(obj, dict):
        if any(k in obj for k in HIST_FIELDS):
            yield path, obj
        for k, v in obj.items():
            yield from find_histogram_records(v, path + (str(k),))
    elif isinstance(obj, list):
        for v in obj:
            yield from find_histogram_records(v, path)


def histogram_points(app_name, record):
    from influxdb_client import Point

    points = []
    for outcome, field in (("accepted", "accepted_adc_histogram"),
                            ("rejected", "rejected_adc_histogram")):
        for bin_idx, count in enumerate(record.get(field) or []):
            if not count:
                continue
            points.append(
                Point("datafilter_adc_histogram")
                .tag("app", app_name)
                .tag("outcome", outcome)
                .tag("bin", str(bin_idx))
                .field("count", int(count))
            )
    return points


def process_chunk(write_api, bucket, org, text):
    """Parse complete JSON-lines from `text` and write any histogram points found."""
    n_written = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            print(f"WARNING: skipping unparseable line: {line[:200]}", file=sys.stderr)
            continue
        for path, hist_record in find_histogram_records(record):
            app_name = path[-1] if path else "DataFilter"
            points = histogram_points(app_name, hist_record)
            if points:
                write_api.write(bucket=bucket, org=org, record=points)
                n_written += len(points)
    return n_written


def tail_forward(path, write_api, bucket, org, poll_interval, once):
    pos = 0
    size = 0
    while True:
        try:
            cur_size = os.path.getsize(path)
        except FileNotFoundError:
            print(f"waiting for {path} to appear...")
            if once:
                return
            time.sleep(poll_interval)
            continue

        if cur_size < size:
            # File was truncated/rewritten (e.g. app restart) -- start over.
            print(f"{path} shrank ({size} -> {cur_size} bytes), re-reading from start")
            pos = 0
        size = cur_size

        if size > pos:
            with open(path, "rb") as f:
                f.seek(pos)
                chunk = f.read()
            last_nl = chunk.rfind(b"\n")
            if last_nl != -1:
                complete, _partial = chunk[:last_nl], chunk[last_nl + 1:]
                pos += last_nl + 1
                n = process_chunk(write_api, bucket, org,
                                   complete.decode("utf-8", errors="replace"))
                if n:
                    print(f"wrote {n} points")

        if once:
            return
        time.sleep(poll_interval)


def main():
    ap = argparse.ArgumentParser(
        description="Forward DataFilter accept/reject ADC histograms (from the "
                    "opmon file sink) to InfluxDB")
    ap.add_argument("--file", default="./info.json",
                    help="opmon file sink path (default: ./info.json, matching "
                         "the session's OpMonURI config)")
    ap.add_argument("--poll-interval", type=float, default=5.0,
                    help="seconds between polls when the file hasn't grown (default: 5, "
                         "overridden by opmon_influx_poll_interval_s when --oks-config is used)")
    ap.add_argument("--once", action="store_true",
                    help="process what's currently new in the file once, then exit")
    ap.add_argument("--oks-config", default=None,
                    help="Path to an OKS data XML file (e.g. dfSession.data.xml) to read "
                         "enable_opmon_influx/opmon_influx_poll_interval_s from, instead "
                         "of requiring --poll-interval. InfluxDB connection details "
                         "(URL/org/bucket/token) always come from INFLUXDB_* environment "
                         "variables -- that's this script's own concern, not DataFilter's.")
    ap.add_argument("--app-id", default="DataFilter_0",
                    help="OKS object id to read attributes from (default: DataFilter_0)")
    ap.add_argument("--check-enabled", action="store_true",
                    help="Print 1/0 for enable_opmon_influx from --oks-config and exit "
                         "immediately, without connecting to InfluxDB or polling")
    args = ap.parse_args()

    oks_attrs = {}
    if args.oks_config:
        try:
            oks_attrs = read_oks_attrs(args.oks_config, "DataFilter", args.app_id)
        except (OSError, ET.ParseError) as e:
            print(f"ERROR: could not read OKS config {args.oks_config}: {e}",
                  file=sys.stderr)
            sys.exit(1)
        if oks_attrs is None:
            print(f"ERROR: object DataFilter/{args.app_id} not found in {args.oks_config}",
                  file=sys.stderr)
            sys.exit(1)

    if args.check_enabled:
        print("1" if oks_bool(oks_attrs.get("enable_opmon_influx", "0")) else "0")
        sys.exit(0)

    poll_interval = args.poll_interval
    if args.oks_config:
        if not oks_bool(oks_attrs.get("enable_opmon_influx", "0")):
            print(f"opmon_to_influx: disabled via OKS config "
                  f"(enable_opmon_influx=false for {args.app_id}), exiting")
            sys.exit(0)
        poll_interval = float(oks_attrs.get("opmon_influx_poll_interval_s",
                                            args.poll_interval))

    url = os.environ.get("INFLUXDB_URL")
    token = os.environ.get("INFLUXDB_TOKEN")
    org = os.environ.get("INFLUXDB_ORG")
    bucket = os.environ.get("INFLUXDB_BUCKET")
    missing = [name for name, val in (("INFLUXDB_URL", url), ("INFLUXDB_TOKEN", token),
                                       ("INFLUXDB_ORG", org), ("INFLUXDB_BUCKET", bucket))
               if not val]
    if missing:
        print(f"ERROR: missing required environment variable(s): {', '.join(missing)}",
              file=sys.stderr)
        sys.exit(1)

    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print(f"forwarding {args.file} -> bucket={bucket} org={org} ({url})")
    tail_forward(args.file, write_api, bucket, org, poll_interval, args.once)


if __name__ == "__main__":
    main()
