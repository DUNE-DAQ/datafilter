"""
compare_filtered_vs_original.py -- per-TriggerRecord comparison between a
DataFilter output file and the original (pre-filter) HDF5 file it came from.

1) Parses the trigger number out of the filtered file's name (FilterResultWriter
   names outputs "..._<trigger_number>.filtered.hdf5").
2) Finds the matching TriggerRecordNNNNNN.MMMM group in the original file.
3) Prints per-fragment and aggregate stats: which detector elements were kept
   vs dropped, and -- for WIBEth fragments -- how many frames survived.

Usage:
  python3 compare_filtered_vs_original.py <original.hdf5> <filtered.hdf5> [<filtered.hdf5> ...]
  python3 compare_filtered_vs_original.py <original.hdf5> <filtered_dir> [--json out.json]
"""

import argparse
import glob
import json
import os
import re
import sys

import h5py

from scan_np04_detect_signals import unpack_frames, FRAG_HDR

TR_GROUP_RE = re.compile(r'^TriggerRecord(\d+)\.(\d+)$')
FILTERED_NAME_RE = re.compile(r'_(\d+)\.filtered\.hdf5$')


def tr_number_from_filename(path):
    """Extract the trigger number FilterResultWriter embeds in its output filename."""
    m = FILTERED_NAME_RE.search(os.path.basename(path))
    if not m:
        raise ValueError(f"cannot find a trigger number in filename: {path}")
    return int(m.group(1))


def find_tr_group(h5file, tr_number):
    """Find the TriggerRecordNNNNNN.MMMM group matching tr_number, regardless
    of how many digits that file pads the record number to."""
    matches = [k for k in h5file.keys()
               if (m := TR_GROUP_RE.match(k)) and int(m.group(1)) == tr_number]
    if not matches:
        return None
    return matches[0]


def fragment_stats(rdata, name):
    """Byte size, and -- for WIBEth datasets -- decoded frame count."""
    raw = bytes(rdata[name][()])
    size = len(raw)
    n_frames = None
    if 'WIBEth' in name and size > FRAG_HDR:
        adcs = unpack_frames(raw[FRAG_HDR:])
        n_frames = 0 if adcs is None else adcs.shape[0]
    return size, n_frames


def compare_one(orig_f, filtered_path):
    tr_number = tr_number_from_filename(filtered_path)
    filt_f = h5py.File(filtered_path, 'r')

    filt_group = find_tr_group(filt_f, tr_number)
    if filt_group is None:
        print(f"[{os.path.basename(filtered_path)}] TR {tr_number}: "
              f"not found inside the filtered file itself (unexpected)")
        filt_f.close()
        return None

    orig_group = find_tr_group(orig_f, tr_number)
    if orig_group is None:
        print(f"[{os.path.basename(filtered_path)}] TR {tr_number}: "
              f"not found in original file -- cannot compare")
        filt_f.close()
        return None

    orig_rdata = orig_f[orig_group + '/RawData']
    filt_rdata = filt_f[filt_group + '/RawData']

    orig_names = set(orig_rdata.keys())
    filt_names = set(filt_rdata.keys())
    dropped = sorted(orig_names - filt_names)
    kept = sorted(orig_names & filt_names)
    unexpected = sorted(filt_names - orig_names)

    rows = []
    orig_bytes_total = filt_bytes_total = 0
    orig_frames_total = filt_frames_total = 0
    for name in sorted(orig_names):
        o_size, o_frames = fragment_stats(orig_rdata, name)
        orig_bytes_total += o_size
        if o_frames is not None:
            orig_frames_total += o_frames
        if name in filt_names:
            f_size, f_frames = fragment_stats(filt_rdata, name)
            filt_bytes_total += f_size
            if f_frames is not None:
                filt_frames_total += f_frames
            rows.append((name, o_size, f_size, o_frames, f_frames))
        else:
            rows.append((name, o_size, 0, o_frames, 0 if o_frames is not None else None))

    filt_f.close()

    print(f"\n=== TR {tr_number} ({os.path.basename(filtered_path)}) ===")
    print(f"original group:  {orig_group}  ({len(orig_names)} fragments)")
    print(f"filtered group:  {filt_group}  ({len(filt_names)} fragments)")
    if unexpected:
        print(f"WARNING: filtered file has fragments absent from the original: {unexpected}")

    print(f"{'fragment':<45} {'orig B':>10} {'filt B':>10} {'orig frames':>12} {'filt frames':>12} {'kept %':>7}")
    for name, o_size, f_size, o_frames, f_frames in rows:
        pct = f"{100.0 * f_frames / o_frames:.0f}%" if o_frames else ("-" if o_frames is None else "0%")
        print(f"{name:<45} {o_size:>10} {f_size:>10} "
              f"{'' if o_frames is None else o_frames:>12} "
              f"{'' if f_frames is None else f_frames:>12} {pct:>7}")

    size_pct = 100.0 * filt_bytes_total / orig_bytes_total if orig_bytes_total else 0.0
    frame_pct = 100.0 * filt_frames_total / orig_frames_total if orig_frames_total else 0.0
    print(f"-- {len(dropped)} fragment(s) fully dropped: {dropped or 'none'}")
    print(f"-- total size: {orig_bytes_total} B -> {filt_bytes_total} B ({size_pct:.1f}% kept)")
    print(f"-- total WIBEth frames: {orig_frames_total} -> {filt_frames_total} ({frame_pct:.1f}% kept)")

    return {
        'tr_number': tr_number,
        'orig_group': orig_group,
        'filt_group': filt_group,
        'n_fragments_orig': len(orig_names),
        'n_fragments_filt': len(filt_names),
        'fragments_dropped': dropped,
        'orig_bytes_total': orig_bytes_total,
        'filt_bytes_total': filt_bytes_total,
        'orig_frames_total': orig_frames_total,
        'filt_frames_total': filt_frames_total,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('original', help='Original (pre-filter) HDF5 file')
    ap.add_argument('filtered', nargs='+',
                     help='One or more *.filtered.hdf5 files, or a directory containing them')
    ap.add_argument('--json', default=None, help='Optional path to dump full per-TR results as JSON')
    args = ap.parse_args()

    filtered_paths = []
    for p in args.filtered:
        if os.path.isdir(p):
            filtered_paths.extend(sorted(glob.glob(os.path.join(p, '*.filtered.hdf5'))))
        else:
            filtered_paths.extend(sorted(glob.glob(p)) or [p])
    if not filtered_paths:
        sys.exit('no filtered files found')

    orig_f = h5py.File(args.original, 'r')
    orig_run = orig_f.attrs.get('run_number')

    results = []
    skipped = 0
    for fp in filtered_paths:
        with h5py.File(fp, 'r') as check_f:
            filt_run = check_f.attrs.get('run_number')
        if orig_run is not None and filt_run != orig_run:
            skipped += 1
            continue
        r = compare_one(orig_f, fp)
        if r:
            results.append(r)
    orig_f.close()

    if skipped:
        print(f"\n(skipped {skipped} file(s) from a different run_number than "
              f"the original's {orig_run})")

    if results:
        n = len(results)
        tot_orig_b = sum(r['orig_bytes_total'] for r in results)
        tot_filt_b = sum(r['filt_bytes_total'] for r in results)
        tot_orig_fr = sum(r['orig_frames_total'] for r in results)
        tot_filt_fr = sum(r['filt_frames_total'] for r in results)
        tot_dropped = sum(len(r['fragments_dropped']) for r in results)
        print(f"\n=== Summary across {n} TR(s) ===")
        print(f"total fragments fully dropped: {tot_dropped}")
        print(f"total size: {tot_orig_b} B -> {tot_filt_b} B "
              f"({100.0 * tot_filt_b / tot_orig_b:.1f}% kept)" if tot_orig_b else "")
        print(f"total WIBEth frames: {tot_orig_fr} -> {tot_filt_fr} "
              f"({100.0 * tot_filt_fr / tot_orig_fr:.1f}% kept)" if tot_orig_fr else "")

    if args.json:
        with open(args.json, 'w') as jf:
            json.dump(results, jf, indent=2)
        print(f"\nSaved -> {args.json}")


if __name__ == '__main__':
    main()
