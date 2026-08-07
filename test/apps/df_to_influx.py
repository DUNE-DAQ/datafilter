#!/usr/bin/env python3
"""
Forward DataFilter's accept/reject ADC histograms to InfluxDB 2.x/3.x.

DataFilter writes these directly to its own dedicated JSON file
(datafilter_adc_histogram.json, in the directory dfcontrol.sh was run from --
same convention as bookkeeping_*.json), rewriting it in full on every opmon
publish cycle. This bypasses opmonlib's normal opmon file sink entirely:
opmonlib's OpMonValue (opmon_entry.proto) only supports scalar field types
(int/uint/double/float/bool/string) -- repeated fields are silently dropped by
the reflection-based Message -> OpMonEntry conversion, so there was no way to
get the full histogram through the real opmon pipeline. See
DataFilter::generate_opmon_data() (plugins/DataFilter.cpp) for the writer side.

File format (flat, single JSON object, fully rewritten each cycle):
  {
    "session": "test-session",
    "app": "DataFilter_0",
    "accepted_adc_histogram": [0, 0, 3, 15, ...],
    "rejected_adc_histogram": [1, 0, 0, 0, ...]
  }

Usage:
  INFLUXDB_URL=https://host:8086 INFLUXDB_TOKEN=... INFLUXDB_ORG=... \\
      INFLUXDB_BUCKET=... python3 df_to_influx.py [--file datafilter_adc_histogram.json]
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


def poll_and_forward(path, write_api, bucket, org, poll_interval, once):
    last_mtime = None
    while True:
        try:
            mtime = os.path.getmtime(path)
        except FileNotFoundError:
            print(f"waiting for {path} to appear...")
            if once:
                return
            time.sleep(poll_interval)
            continue

        if mtime != last_mtime:
            last_mtime = mtime
            try:
                with open(path, "r") as f:
                    record = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                # File may be mid-write (DataFilter rewrites it in full each
                # cycle); just retry next poll rather than treating this as fatal.
                print(f"WARNING: could not read/parse {path}: {e}", file=sys.stderr)
            else:
                app_name = record.get("app", "DataFilter")
                points = histogram_points(app_name, record)
                if points:
                    write_api.write(bucket=bucket, org=org, record=points)
                    print(f"wrote {len(points)} points")

        if once:
            return
        time.sleep(poll_interval)


def main():
    ap = argparse.ArgumentParser(
        description="Forward DataFilter accept/reject ADC histograms "
                    "(from its own dedicated JSON file) to InfluxDB")
    ap.add_argument("--file", default="./datafilter_adc_histogram.json",
                    help="path to the histogram file DataFilter writes "
                         "(default: ./datafilter_adc_histogram.json)")
    ap.add_argument("--poll-interval", type=float, default=5.0,
                    help="seconds between polls when the file hasn't changed (default: 5, "
                         "overridden by df_influx_poll_interval_s when --oks-config is used)")
    ap.add_argument("--once", action="store_true",
                    help="process the file once (if changed), then exit")
    ap.add_argument("--oks-config", default=None,
                    help="Path to an OKS data XML file (e.g. dfSession.data.xml) to read "
                         "enable_df_influx/df_influx_poll_interval_s from, instead "
                         "of requiring --poll-interval. InfluxDB connection details "
                         "(URL/org/bucket/token) always come from INFLUXDB_* environment "
                         "variables -- that's this script's own concern, not DataFilter's.")
    ap.add_argument("--app-id", default="DataFilter_0",
                    help="OKS object id to read attributes from (default: DataFilter_0)")
    ap.add_argument("--check-enabled", action="store_true",
                    help="Print 1/0 for enable_df_influx from --oks-config and exit "
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
        print("1" if oks_bool(oks_attrs.get("enable_df_influx", "0")) else "0")
        sys.exit(0)

    poll_interval = args.poll_interval
    if args.oks_config:
        if not oks_bool(oks_attrs.get("enable_df_influx", "0")):
            print(f"df_to_influx: disabled via OKS config "
                  f"(enable_df_influx=false for {args.app_id}), exiting")
            sys.exit(0)
        poll_interval = float(oks_attrs.get("df_influx_poll_interval_s",
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
    poll_and_forward(args.file, write_api, bucket, org, poll_interval, args.once)


if __name__ == "__main__":
    main()
