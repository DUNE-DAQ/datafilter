import argparse
import h5py, os, json
import numpy as np

parser = argparse.ArgumentParser(description='Scan HDF5 file for max ADC values')
parser.add_argument('fname', help='HDF5 file to scan')
parser.add_argument('--thresh', type=int, default=9145, help='ADC threshold (default: 9145)')
parser.add_argument('--header', action='store_true',
                    help='Print HDF5 structure and attribute probe before scanning')
args = parser.parse_args()
fname = args.fname
THRESH = args.thresh

f = h5py.File(fname, 'r')


def _fmt_attr(v):
    # Decode bytes/numpy bytes to string first
    if isinstance(v, (bytes, np.bytes_)):
        try:
            v = v.decode('utf-8')
        except UnicodeDecodeError:
            return f"<bytes len={len(v)}>"
    if isinstance(v, np.ndarray):
        if v.dtype.kind in ('S', 'U'):  # byte-string or unicode array
            v = v.flat[0] if v.size == 1 else str(v.tolist())
        else:
            return f"<array shape={v.shape} dtype={v.dtype} [{' '.join(str(x) for x in v.flat[:4])}{'...' if v.size > 4 else ''}]>"
    if isinstance(v, str):
        s = v.strip()
        if s.startswith(('{', '[')):
            try:
                return '\n' + '\n'.join('      ' + l for l in json.dumps(json.loads(s), indent=2).splitlines())
            except json.JSONDecodeError:
                pass
        return s
    return str(v)


def _print_attrs(obj, label):
    attrs = dict(obj.attrs)
    if attrs:
        print(f"  {label} attributes ({len(attrs)}):")
        for k, v in attrs.items():
            if k == 'source_id_geo_id_map':
                continue
            print(f"    {k} = {_fmt_attr(v)}")
    else:
        print(f"  {label} attributes: (none)")


def print_adc_hist(maxes, n_bins=10):
    if not maxes:
        print("max_adc distribution: (no data)")
        return
    lo, hi = min(maxes), max(maxes)
    med = sorted(maxes)[len(maxes) // 2]
    print(f"max_adc  count={len(maxes)}  min={lo}  median={med}  max={hi}")
    if lo == hi:
        return
    width = (hi - lo) / n_bins
    bins = [0] * n_bins
    for v in maxes:
        idx = min(int((v - lo) / width), n_bins - 1)
        bins[idx] += 1
    bar_max = max(bins)
    for i, cnt in enumerate(bins):
        low  = int(lo + i * width)
        high = int(lo + (i + 1) * width) - 1
        bar  = '#' * (cnt * 30 // bar_max) if bar_max else ''
        print(f"  [{low:5d}-{high:5d}] {cnt:4d}  {bar}")


def probe_hdf5(h5):
    keys = list(h5.keys())
    print(f"\n--- HDF5 structure probe ---")
    print(f"Top-level groups ({len(keys)}): {keys[:10]}{'...' if len(keys) > 10 else ''}")
    _print_attrs(h5, 'file root')
    tr_keys = [k for k in keys if k.startswith('TriggerRecord')]
    print(f"TriggerRecord groups found: {len(tr_keys)}")
    if not tr_keys:
        prefixes = sorted(set(k.split('.')[0] for k in keys))[:8]
        print(f"  (no TriggerRecord groups; name prefixes: {prefixes})")
        print("--- end probe ---\n")
        return
    first = tr_keys[0]
    sub = list(h5[first].keys())
    print(f"First TR '{first}' subgroups: {sub}")
    if 'RawData' in sub:
        raw = list(h5[first + '/RawData'].keys())
        wib = [k for k in raw if 'WIBEth' in k]
        print(f"  RawData datasets ({len(raw)}): {raw[:6]}{'...' if len(raw) > 6 else ''}")
        print(f"  WIBEth fragments: {len(wib)}")
        if not wib:
            print(f"  (no WIBEth; sample names: {raw[:4]})")
    else:
        print(f"  WARNING: no 'RawData' subgroup; actual subgroups: {sub}")
    print("--- end probe ---\n")


if args.header:
    probe_hdf5(f)

SAMPLES, CHANNELS, BITS = 64, 64, 14
N_WORDS = (CHANNELS * BITS + 63) // 64   # = 14
FRAME_HDR = 32   # 16 B DAQEthHeader + 16 B WIBEthHeader
FRAME_SIZE = FRAME_HDR + SAMPLES * N_WORDS * 8   # = 7200
FRAG_HDR = 72
MASK14 = 0x3FFF

# Precomputed per-channel extraction tables (constant for all WIBEth frames)
FRAME_WORDS = FRAME_SIZE // 8                                          # 900 uint64 words per frame
HDR_WORDS   = FRAME_HDR  // 8                                          # 4 header words to skip
_CH         = np.arange(CHANNELS, dtype=np.uint64)
_WORD_IDX   = ((np.uint64(BITS) * _CH) // np.uint64(64)).astype(np.intp)  # which uint64 word
_BIT_OFF    = ((np.uint64(BITS) * _CH) % np.uint64(64))                   # bit offset within that word
_SPANS      = (_BIT_OFF + np.uint64(BITS)) > np.uint64(64)                # channels spanning two words
_MASK14     = np.uint64(MASK14)


def frag_max_adc(payload):
    n = len(payload) // FRAME_SIZE
    if n == 0:
        return None
    # Read all frames as little-endian uint64, reshape to (n_frames, 900)
    words = np.frombuffer(payload[:n * FRAME_SIZE], dtype='<u8').reshape(n, FRAME_WORDS)
    # Drop per-frame header words -> (n_frames, SAMPLES, N_WORDS)
    data = words[:, HDR_WORDS:].reshape(n, SAMPLES, N_WORDS)
    # Vectorized 14-bit extraction for all 64 channels -> (n_frames, SAMPLES, 64)
    adcs = (data[:, :, _WORD_IDX] >> _BIT_OFF) & _MASK14
    # Fix up channels whose value spans two consecutive uint64 words
    if _SPANS.any():
        sl = _BIT_OFF[_SPANS]
        adcs[:, :, _SPANS] |= (data[:, :, _WORD_IDX[_SPANS] + 1] << (np.uint64(64) - sl)) & _MASK14
    return int(adcs.max())

# Per-TR stats
tr_results = {}  # tr_name -> [max_adc_per_wibeth_fragment]
for tr_name in sorted(f.keys()):
    if not tr_name.startswith('TriggerRecord'):
        continue
    rdata = f[tr_name + '/RawData']
    frags = []
    for ds in rdata:
        if 'WIBEth' not in ds:
            continue
        data = bytes(rdata[ds][()])
        if len(data) <= FRAG_HDR:
            continue
        mx = frag_max_adc(data[FRAG_HDR:])
        if mx is not None:
            frags.append(mx)
    if frags:
        tr_results[tr_name] = frags

f.close()

# Summary
all_maxes = [m for frags in tr_results.values() for m in frags]
print(f"File: {os.path.basename(fname)}")
print(f"Total TRs with WIBEth: {len(tr_results)}")
print(f"Total fragments: {len(all_maxes)}")
print_adc_hist(all_maxes)
print()

n_tr_dropped = sum(all(m < THRESH for m in f) for f in tr_results.values())
n_tr_kept    = sum(all(m >= THRESH for m in f) for f in tr_results.values())
n_tr_partial = len(tr_results) - n_tr_dropped - n_tr_kept

total = len(tr_results)
print(f"With adc_threshold={THRESH}:")
print(f"  TRs fully dropped  (all WIBEth fail): {n_tr_dropped}/{total}")
print(f"  TRs partially kept (some WIBEth fail): {n_tr_partial}/{total}")
print(f"  TRs fully kept     (all WIBEth pass):  {n_tr_kept}/{total}")
