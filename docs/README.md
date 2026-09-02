# DataFilter V5 — Integration Test Guide

* For old V4:
  * https://github.com/DUNE-DAQ/datafilter/tree/dunedaq-v4.1.1/
  * https://github.com/wchen2013a/dfbackend/tree/dunedaq-v4.1.1

---

## Quickstart

Setup the DataFilter first. 

```bash
wget https://raw.githubusercontent.com/DUNE-DAQ/datafilter/refs/heads/develop/setup-datafilter.sh
# The $INSTALL_DIR variable need to be defined before running the script.
chmod 755 setup-datafilter.sh
./setup-datafilter.sh /your/installation/path  
```
The script will build the project. Once it is done.

Open a terminals and run the apps from `test/apps/`,
you need to run trdispatcher, filterorchestrator, datafilter2 and filterresultwriter:

```bash

export DATAFILTER_WORK_DIR=/your/installation/path

cd test/apps   # in each terminal

# to build DF (after dbt-build + dbt-workarea-env, dfcontrol.sh resolves via PATH
# to the installed copy at install/datafilter/bin/dfcontrol.sh; before that, or if
# it's not on PATH, run it via ./dfcontrol.sh from test/apps/)

dbt-build or dfcontrol.sh build

# to start all the four apps

dfcontrol.sh start

# to stop all the four apps

dfcontrol.sh stop

# you can also start an individual app, for example trdispatcher (trd)

dfcontrol.sh start trd # this is the short name. You can use the long name, trdispatcher if you want. 

# if you want the apps to be supervise

dfcontrol.sh supervise

# to stop the active watchdog (supervisord or in-house, whichever is running)

dfcontrol.sh supervise stop

# you can monitor the logs and the outputs, tmux is required.

dfcontrol.sh monitor

# show run status

dfcontrol.sh status

# to get help

./dfcontrol.sh 

```

**Logs and bookkeeping output location:** `logs/` and `bookkeeping_*.json` are written to
the directory `dfcontrol.sh` is invoked from, not a fixed path under the source tree. In
the example above (`cd test/apps` first), that means output lands in `test/apps/`; run
from a different directory to keep separate test runs' output apart.

All configuration lives in `test/config/dfSession.data.xml`.

---

## Configuration Reference

### 1. TRDispatcher (`TRDispatcher_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `storage_pathname` | string | `/lcg/storage19/test-area/dune/trigger_records/sourcehdf5` | Directory containing source HDF5 files |
| `is_from_storage` | bool | `0` | `1` = read real HDF5 files from `storage_pathname`. **Takes precedence** — the `generate_*` flags below are ignored (and a warning logged) when this is `1` |
| `input_h5_filename` | string | `np04hd_run024552_0011_...hdf5` | **Not used for dispatch.** Files are discovered by scanning `storage_pathname`; `json_file` decides which are new |
| `json_file` | string | `hdf5_files_list.json` | JSON file tracking which source files have already been processed |
| `generate_trigger_record` | bool | `1` | `1` = generate synthetic TRs. Only effective when `is_from_storage=0` |
| `generate_time_slice` | bool | `1` | `1` = generate synthetic TSs. Only effective when `is_from_storage=0` |
| `parallel_send` | bool | `1` | Generated mode only: `1` dispatches a TR and a TS together on separate threads per request (`kGeneratedParallel`); `0` dispatches them sequentially (`kGeneratedSerial`) |
| `number_generated_events` | u32 | `1000` | Generated mode only: max total generated sends. **Shared** between TR and TS (one counter decremented by both). `0` = unlimited |
| `generated_window` | u32 | `20` | Generated mode only: max number of dispatched TR/TS. Also the batch size used to group bookkeeping JSON output (see below). Ignored in storage mode |
| `send_timeout_ms` | u32 | `1000` | Send timeout in ms |
| `recv_timeout_ms` | u32 | `1000` | Receive timeout in ms |

**Dispatch mode selection:** `is_from_storage` is checked first and wins outright. If it
is `1`, TRD always reads from `storage_pathname` and logs a warning if either `generate_*`
flag was also set. Only when `is_from_storage=0` do the generate flags choose a mode:
both set (with `parallel_send=1`) gives parallel TR+TS generation, otherwise serial;
neither set falls back to reading from storage.

In storage mode TRD always scans `storage_pathname` and dispatches every `*.hdf5` file
there that is not already listed in `json_file`, regardless of `input_h5_filename`. Files
still being written (`*.writing`) and already-filtered output (`*.filtered.*`) are
skipped, as is any file modified within the last hour.

**Note on `hdf5_files_list.json`:** TRD skips any file already listed in this file.
Remove an entry before re-running to reprocess that file. The entry is re-added
automatically after a successful run. You can also add new HDF5 files, it will process automatically.

**Generated-mode dispatch pacing (`generated_window`):** unlike storage mode (which
dispatches one HDF5 file at a time and waits for it to complete before scanning for the
next), generated mode has no natural "one file in flight" limit. `generated_window`
bounds how many TR (and, independently, how many TS) cycles can be dispatched but not
yet confirmed written at once — TRDispatcher blocks further dispatch of a type once its
window is full, and a slot frees up as each cycle's completion is confirmed. Raise it for
more throughput at the cost of more concurrent in-flight state; the schema default is
`4`, currently configured here as `20`.

**Generated-mode bookkeeping JSON files:** `generated_window` also sets how many cycles'
worth of bookkeeping is grouped into one file. Instead of one `bookkeeping_*.json` per
TR/TS (which would produce thousands of tiny files over a long run), TR and TS each get
their own file per batch — `bookkeeping_<run>_<batch>_TR.json` /
`_TS.json` — each holding `generated_window` dispatch entries, a single aggregate
completion entry (summed counts plus every written file's `trigger_number`/`ts_number`),
and `generated_window` write-confirmation entries. TR and TS are kept in separate files
because their sequence counters are independent and can otherwise land on the same batch
number by coincidence. A batch's completion entry shows `write_failed` instead of
`file_completed` if any record in it was dropped or never arrived in time — this is a
real signal of lost/orphaned data, not a formatting issue, so check
`total_trs_written`/`total_ts_written` against `trs_dispatched_by_trd`/`expected_ts` in
that entry when you see it. On each `do_start()`, TRDispatcher deletes any leftover
`bookkeeping_<run>_*.json` files for its (generated-mode) run number before dispatching,
so re-running with the same `run_number` never merges stale data from a previous session
into the new run's files.

---

### 2. DataFilter (`DataFilter_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `adc_threshold` | u16 | `9130` | ADC threshold for TR filtering |
| `enable_df_influx` | bool | `true` | If `true`, `dfcontrol.sh` launches `df_to_influx.py` alongside DataFilter to forward its accept/reject ADC histograms to InfluxDB |
| `df_influx_poll_interval_s` | u32 | `5` | Seconds between DataFilter's histogram-file writes / `df_to_influx.py`'s polls |
| `enable_frame_filter` | bool | `false` | Filtering granularity — `false` = whole fragments, `true` = rebuild fragments from surviving frames (see below) |
| `prefetch_window` | u32 | `8` | How many `next_tr`/`next_ts` requests DataFilterReceiver pre-issues to FilterOrchestrator, refilled one-for-one as each TR/TS is ingested. Bounds how far TRDispatcher can run ahead of DataFilter |

**ADC threshold semantics:** A trigger record is kept if any channel/sample in any WIBEth
fragment has a 14-bit ADC value `>= adc_threshold`. A TR is dropped only when **all** its
WIBEth fragments fail the threshold.

| Threshold range | Effect on `np04hd_run024552_0011` (28 TRs) |
|---|---|
| `≤ 9123` | All 28 TRs kept |
| `9130` | 27 kept, 1 dropped |
| `9145` | 20 kept, 8 dropped |
| `≥ 9170` | All 28 TRs dropped |

For fully saturated ADC data (e.g. `swtest_run001039`), max ADC = 16383; use a threshold
`> 16383` to drop all, or any value `≤ 16383` to keep all.

**Filtering granularity (`enable_frame_filter`):** a WIBEth fragment is a sequence of
7200-byte `WIBEthFrame`s (64 channels x 64 time samples each).

| | `false` (default) | `true` |
|---|---|---|
| Filter unit | whole fragment | individual frame |
| Kept if | any sample in the *fragment* `>= adc_threshold` | any sample in *that frame* `>= adc_threshold` |
| Output fragment | unchanged | rebuilt with only surviving frames (smaller) |
| Histogram entries | one per fragment (its overall max ADC) | one per **frame** |

Frame mode gives real data reduction inside a fragment and far richer histogram
statistics — a ~840 KB fragment holds ~117 frames, so it contributes ~117 histogram
entries instead of 1. Expect a correspondingly larger InfluxDB write volume. A fragment
whose frames all fail is dropped entirely; if every WIBEth fragment in a TR is dropped,
the whole TR is dropped (same rule as fragment mode). Payload bytes beyond the last whole
frame are not carried into a rebuilt fragment.

**Forwarding accept/reject ADC histograms to InfluxDB (`df_to_influx.py`):**
DataFilter accumulates two histograms of `max_adc` (one for accepted, one for rejected),
written to its own `datafilter_adc_histogram.json` file every
`df_influx_poll_interval_s`. The entry granularity follows `enable_frame_filter` — one
entry per WIBEth **fragment** when `false`, one per **frame** when `true` (see
"Filtering granularity" above). This bypasses the normal opmon pipeline entirely —
opmonlib's `OpMonValue` only supports scalar field types, so a `repeated` field would be
silently dropped by the reflection-based conversion to `OpMonEntry`. This is opt-in and
off by default (`enable_df_influx=false`), since computing it disables the early-exit
optimization in the ADC threshold check. To enable:

1. Set `enable_df_influx=true` on `DataFilter_0` in `dfSession.data.xml`.
2. Export the InfluxDB 2.x/3.x connection details as environment variables before
   running `dfcontrol.sh` — these are `df_to_influx.py`'s own concern, not stored in
   OKS config: `INFLUXDB_URL`, `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`.
   `test/apps/setup_influx.sh` does this for you: copy
   `setup_influx.local.sh.example` to `setup_influx.local.sh` (gitignored) with your
   real values, then `source setup_influx.sh` each session.
3. `dfcontrol.sh start` (or `restart`) will then also launch `df_to_influx.py`
   automatically; `dfcontrol.sh status` shows it alongside the four apps.

If any `INFLUXDB_*` variable is missing while `enable_df_influx=true`,
`df_to_influx.py` logs a clear error and exits rather than silently doing nothing.

---

### 3. FilterResultWriter (`FilterResultWriter_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `odir` | string | `/lcg/storage18/dune/chen` | Output directory for filtered HDF5 files |
| `output_h5_filename` | string | `datafilter_output_h5_test` | Output filename prefix |
| `min_free_bytes` | u64 | `1024` | Minimum free disk space (bytes) required before FRW will write; set to `2147483648` (2 GB) for production |
| `send_timeout_ms` | u32 | `1000` | Send timeout in ms |
| `recv_timeout_ms` | u32 | `1000` | Receive timeout in ms |

---

### 4. FilterOrchestrator (`FilterOrchestrator_0`)

| Attribute | Type | Current value | Description |
|---|---|---|---|
| `send_timeout_ms` | u32 | `1000` | Send timeout in ms |
| `recv_timeout_ms` | u32 | `0` | Receive timeout in ms (`0` = blocking) |

---

## Network Connection Port Map

All 15 `NetworkConnection` objects in `dfSession.data.xml`, with their ports, types, and roles:

| Connection ID | Port | Type | Direction | Purpose |
|---|---|---|---|---|
| `conn_A0_G0_C0_` | 15500 | kPubSub | TRD → DF | TriggerRecord data |
| `conn_A1_G0_C0_` | 15501 | kPubSub | DF → FRW | Filtered TriggerRecord data |
| `ts_conn_A0_G0_C0_` | 15510 | kPubSub | TRD → DF | TimeSlice data |
| `ts_conn_A1_G0_C0_` | 15511 | kPubSub | DF → FRW | Filtered TimeSlice data |
| `FO_ctrl0` | 12000 | kSendRecv | DF → FO | DF status feedback to FO |
| `TR_tracking0` | 13000 | kSendRecv | FRW ↔ TRD/FO | TR completion notifications (bidirectional) |
| `TR_tracking1` | 13001 | kSendRecv | → TRD | TRD handshake channel |
| `TR_tracking2` | 13002 | kSendRecv | → DF | DF handshake channel |
| `trdispatcher0` | 23000 | kSendRecv | FO → TRD | FO dispatch control to TRD |
| `trdispatcher1` | 23001 | kSendRecv | DF → FO | DF status to FO |
| `trwriter0` | 24000 | kSendRecv | DF → FRW | TR write command |
| `tswriter0` | 24001 | kSendRecv | DF → FRW | TS write command |
| `bookkeeping0` | 33000 | kSendRecv | TRD+FRW → DF | Bookkeeping messages to DF |
| `bookkeeping1` | 33001 | kSendRecv | DF → FRW | Initial BK metadata (run number, file attributes) |
| `bookkeeping2` | 33002 | kSendRecv | DF → TRD | Write confirmation (DF forwards FRW completion) |

---

## Multi-host IP Configuration

### Address format

Each `NetworkConnection` has an `address` attribute of the form:

```
tcp://<IP>:<PORT>
```

The default IP `127.0.0.1` works when all four apps run on the same host.
To split apps across servers, update the `address` on the relevant connections.

### Binding rules

| Connection type | Who binds | IP to use |
|---|---|---|
| `kSendRecv` | The **receiver** module binds | Use the receiver's host IP |
| `kPubSub` | The **publisher** module binds | Use the publisher's host IP |

### Example: 2-host deployment

**server1** (`10.0.0.14`): runs `trdispatcher`
**server2** (`10.0.0.13`): runs `filterorchestrator`, `datafilter2`, `filterresultwriter`

Update the `address` attribute of each `NetworkConnection` object in `dfSession.data.xml`:

| Connection ID | Port | Binder | New address |
|---|---|---|---|
| `conn_A0_G0_C0_` | 15500 | TRD (publisher) | `tcp://10.0.0.14:15500` |
| `ts_conn_A0_G0_C0_` | 15510 | TRD (publisher) | `tcp://10.0.0.14:15510` |
| `trdispatcher0` | 23000 | TRD (receiver) | `tcp://10.0.0.14:23000` |
| `TR_tracking1` | 13001 | TRD (receiver) | `tcp://10.0.0.14:13001` |
| `bookkeeping2` | 33002 | TRD (receiver) | `tcp://10.0.0.14:33002` |
| `conn_A1_G0_C0_` | 15501 | DF (publisher) | `tcp://10.0.0.13:15501` |
| `ts_conn_A1_G0_C0_` | 15511 | DF (publisher) | `tcp://10.0.0.13:15511` |
| `TR_tracking2` | 13002 | DF (receiver) | `tcp://10.0.0.13:13002` |
| `trdispatcher1` | 23001 | FO (receiver) | `tcp://10.0.0.13:23001` |
| `FO_ctrl0` | 12000 | FO (receiver) | `tcp://10.0.0.13:12000` |
| `TR_tracking0` | 13000 | FRW (receiver) | `tcp://10.0.0.13:13000` |
| `trwriter0` | 24000 | FRW (receiver) | `tcp://10.0.0.13:24000` |
| `tswriter0` | 24001 | FRW (receiver) | `tcp://10.0.0.13:24001` |
| `bookkeeping0` | 33000 | DF (receiver) | `tcp://10.0.0.13:33000` |
| `bookkeeping1` | 33001 | FRW (receiver) | `tcp://10.0.0.13:33001` |

**Firewall:** both servers must be able to reach each other on all 15 ports listed above.
