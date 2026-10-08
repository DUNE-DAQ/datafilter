"""
scan_np04_detect_signals.py -- per-channel spike / real-signal detection over
a raw (pre-filter) WIBEth HDF5 file.

Usage:
  python3 scan_np04_detect_signals.py <file.hdf5> [--sigma 5.0]
      [--direction above|below|both] [--min-width 1] [--thresh N]
      [--channel SPEC] [--record NAME] [--frag NAME] [--top 10]
      [--json out.json]
"""

import argparse
import json
import os

import h5py
import numpy as np

# ---------------------------------------------------------------------------
# WIBEth frame layout constants -- match scan_np04hd.py / plot_frame_filter.py
# exactly, which in turn match fddetdataformats/WIBEthFrame.hpp and
# detdataformats/DAQEthHeader.hpp.
# ---------------------------------------------------------------------------
SAMPLES, CHANNELS, BITS = 64, 64, 14
N_WORDS = (CHANNELS * BITS + 63) // 64          # = 14
FRAME_HDR = 32                                  # 16 B DAQEthHeader + 16 B WIBEthHeader
FRAME_SIZE = FRAME_HDR + SAMPLES * N_WORDS * 8  # = 7200, sizeof(WIBEthFrame)
FRAG_HDR = 72                                   # raw Fragment header size before frame data starts
MASK14 = 0x3FFF

FRAME_WORDS = FRAME_SIZE // 8   # 900 uint64 words per frame
HDR_WORDS = FRAME_HDR // 8      # 4 header words to skip per frame

# Precomputed per-channel extraction tables (constant for all WIBEth frames)
_CH = np.arange(CHANNELS, dtype=np.uint64)
_WORD_IDX = ((np.uint64(BITS) * _CH) // np.uint64(64)).astype(np.intp)
_BIT_OFF = ((np.uint64(BITS) * _CH) % np.uint64(64))
_SPANS = (_BIT_OFF + np.uint64(BITS)) > np.uint64(64)
_MASK14 = np.uint64(MASK14)

# MAD -> sigma conversion factor for a normal distribution.
MAD_TO_SIGMA = 1.4826


def unpack_frames(payload):
    """Vectorized decode of a WIBEth fragment payload (post FRAG_HDR strip).

    Returns adcs: (n_frames, SAMPLES, CHANNELS) uint16, or None if payload
    holds no complete frame.
    """
    n = len(payload) // FRAME_SIZE
    if n == 0:
        return None

    words = np.frombuffer(payload[:n * FRAME_SIZE], dtype='<u8').reshape(n, FRAME_WORDS)
    data = words[:, HDR_WORDS:].reshape(n, SAMPLES, N_WORDS)
    adcs = (data[:, :, _WORD_IDX] >> _BIT_OFF) & _MASK14
    if _SPANS.any():
        sl = _BIT_OFF[_SPANS]
        adcs[:, :, _SPANS] |= (data[:, :, _WORD_IDX[_SPANS] + 1] << (np.uint64(64) - sl)) & _MASK14

    return adcs.astype(np.uint16)


def parse_channel_spec(spec, n_channels):
    """Parse '5', '0-7', or '0-3,10,20-22' into a sorted list of unique channel ints."""
    chans = set()
    for part in spec.split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            lo_s, hi_s = part.split('-', 1)
            lo, hi = int(lo_s), int(hi_s)
            if lo > hi:
                lo, hi = hi, lo
            chans.update(range(lo, hi + 1))
        else:
            chans.add(int(part))
    bad = sorted(c for c in chans if c < 0 or c >= n_channels)
    if bad:
        raise SystemExit(f'--channel: index out of range [0,{n_channels - 1}]: {bad}')
    return sorted(chans)


def count_spikes(mask_1d, min_width):
    """Count runs of >= min_width consecutive True values in a 1D bool array."""
    if not mask_1d.any():
        return 0
    if min_width <= 1:
        # Fast path: rising-edge count (True preceded by False or array start).
        edges = mask_1d & ~np.concatenate(([False], mask_1d[:-1]))
        return int(edges.sum())
    # Explicit run-length pass, only taken when min_width > 1 is requested.
    padded = np.concatenate(([False], mask_1d, [False]))
    diff = np.diff(padded.astype(np.int8))
    starts = np.flatnonzero(diff == 1)
    ends = np.flatnonzero(diff == -1)
    return int(np.sum((ends - starts) >= min_width))


def channel_spike_mask(waveform, pedestal, robust_sigma, sigma, direction):
    dev = waveform - pedestal
    thresh = sigma * robust_sigma
    if direction == 'above':
        return dev >= thresh
    if direction == 'below':
        return dev <= -thresh
    return np.abs(dev) >= thresh


def main():
    ap = argparse.ArgumentParser(
        description="Detect per-channel ADC spikes / real signals in a raw WIBEth HDF5 file, "
                    "relative to each channel's own pedestal (median + MAD-based sigma), "
                    "as opposed to scan_np04hd.py's flat, fragment-level threshold.")
    ap.add_argument('fname', help='HDF5 file to scan (RAW/pre-filter file, not FRW output)')
    ap.add_argument('--sigma', type=float, default=5.0,
                     help='Statistical threshold multiplier: pedestal +/- sigma * robust_sigma (default: 5.0)')
    ap.add_argument('--direction', choices=('above', 'below', 'both'), default='above',
                     help="Spike polarity: 'above' matches the existing filter's >= convention (default), "
                          "'below' for dips, 'both' for either")
    ap.add_argument('--min-width', type=int, default=1,
                     help='Minimum consecutive over-threshold samples to count as one spike (default: 1)')
    ap.add_argument('--thresh', type=int, default=None,
                     help="Optional flat ADC cross-check (no default -- opt-in secondary comparison "
                          "against the existing filter's style of threshold)")
    ap.add_argument('--channel', default=None,
                     help='Restrict to specific channel(s): a single index (5), a range (0-7), or a '
                          'comma list mixing both (0-3,10,20-22). Default: all 64 channels')
    ap.add_argument('--record', default=None, help='Restrict to one TriggerRecord group (default: all)')
    ap.add_argument('--frag', default=None, help='Restrict to one WIBEth dataset name (default: all)')
    ap.add_argument('--top', type=int, default=10,
                     help='How many highest-spike-count channels to print (default: 10)')
    ap.add_argument('--json', default=None, help='Optional path to dump full per-channel results as JSON')
    args = ap.parse_args()

    ch_list = parse_channel_spec(args.channel, CHANNELS) if args.channel is not None else list(range(CHANNELS))

    f = h5py.File(args.fname, 'r')

    n_spikes = np.zeros(CHANNELS, dtype=np.int64)
    n_thresh = np.zeros(CHANNELS, dtype=np.int64) if args.thresh is not None else None
    peak_adc = np.full(CHANNELS, -1, dtype=np.int64)
    peak_dev = np.full(CHANNELS, -1.0, dtype=np.float64)
    peak_sigma = np.full(CHANNELS, -1.0, dtype=np.float64)  # peak_dev / robust_sigma at the peak
    peak_loc = [None] * CHANNELS  # (tr_name, frag_name, frame_idx)
    pedestal_sum = np.zeros(CHANNELS, dtype=np.float64)
    n_fragments_per_channel = np.zeros(CHANNELS, dtype=np.int64)

    n_tr, n_frag, n_samples_total = 0, 0, 0

    for tr_name in sorted(f.keys()):
        if not tr_name.startswith('TriggerRecord'):
            continue
        if args.record is not None and tr_name != args.record:
            continue
        rdata_path = tr_name + '/RawData'
        if rdata_path not in f:
            continue
        rdata = f[rdata_path]
        tr_has_frag = False
        for ds in rdata:
            if 'WIBEth' not in ds:
                continue
            if args.frag is not None and ds != args.frag:
                continue
            raw = bytes(rdata[ds][()])
            if len(raw) <= FRAG_HDR:
                continue
            adcs = unpack_frames(raw[FRAG_HDR:])
            if adcs is None:
                continue

            tr_has_frag = True
            n_frag += 1
            n_frames = adcs.shape[0]
            n_samples_total += n_frames * SAMPLES

            flat = adcs.reshape(n_frames * SAMPLES, CHANNELS).astype(np.float64)
            pedestal = np.median(flat, axis=0)
            mad = np.median(np.abs(flat - pedestal), axis=0)
            robust_sigma = np.maximum(MAD_TO_SIGMA * mad, 1e-9)  # avoid div-by-zero on a dead-flat channel

            for c in ch_list:
                waveform = flat[:, c]
                mask = channel_spike_mask(waveform, pedestal[c], robust_sigma[c], args.sigma, args.direction)
                n_spikes[c] += count_spikes(mask, args.min_width)
                pedestal_sum[c] += pedestal[c]
                n_fragments_per_channel[c] += 1

                if mask.any():
                    dev = np.abs(waveform - pedestal[c])
                    candidate_idx = np.flatnonzero(mask)
                    local_idx = candidate_idx[np.argmax(dev[candidate_idx])]
                    local_dev = dev[local_idx]
                    if local_dev > peak_dev[c]:
                        peak_dev[c] = local_dev
                        peak_sigma[c] = local_dev / robust_sigma[c]
                        peak_adc[c] = int(waveform[local_idx])
                        peak_loc[c] = (tr_name, ds, int(local_idx // SAMPLES))

                if args.thresh is not None:
                    n_thresh[c] += int((waveform >= args.thresh).sum())

        if tr_has_frag:
            n_tr += 1

    f.close()

    if n_frag == 0:
        print(f'{os.path.basename(args.fname)}: no WIBEth fragments matched (check --record/--frag)')
        return

    avg_pedestal = np.divide(pedestal_sum, n_fragments_per_channel,
                              out=np.zeros_like(pedestal_sum), where=n_fragments_per_channel > 0)

    print(f'File: {os.path.basename(args.fname)}')
    print(f'TRs scanned: {n_tr}  fragments: {n_frag}  samples/channel: {n_samples_total}')
    print(f'Detection: direction={args.direction}  sigma={args.sigma}  min_width={args.min_width}'
          + (f'  thresh={args.thresh}' if args.thresh is not None else ''))
    print()

    order = sorted(ch_list, key=lambda c: n_spikes[c], reverse=True)
    n_quiet = sum(1 for c in ch_list if n_spikes[c] == 0)
    print(f'{len(ch_list)} channels scanned, {n_quiet} with zero spikes')
    print()

    header = f'{"ch":>4}  {"n_spikes":>9}  {"pedestal":>9}  {"peak_adc":>9}  {"peak_sigma":>10}'
    if args.thresh is not None:
        header += f'  {"n_thresh":>9}'
    header += '  first_seen_at'
    print(header)
    for c in order[:args.top]:
        loc = peak_loc[c]
        loc_str = f'{loc[0]} {loc[1]} frame {loc[2]}' if loc else '-'
        sigma_str = f'{peak_sigma[c]:>10.2f}' if peak_sigma[c] >= 0 else f'{"-":>10}'
        row = f'{c:>4}  {n_spikes[c]:>9}  {avg_pedestal[c]:>9.1f}  {peak_adc[c]:>9}  {sigma_str}'
        if args.thresh is not None:
            row += f'  {n_thresh[c]:>9}'
        row += f'  {loc_str}'
        print(row)

    if args.json:
        result = {
            'file': os.path.basename(args.fname),
            'n_tr': n_tr,
            'n_fragments': n_frag,
            'n_samples_per_channel': n_samples_total,
            'sigma': args.sigma,
            'direction': args.direction,
            'min_width': args.min_width,
            'thresh': args.thresh,
            'channels': [
                {
                    'channel': c,
                    'n_spikes': int(n_spikes[c]),
                    'avg_pedestal': float(avg_pedestal[c]),
                    'peak_adc': int(peak_adc[c]) if peak_adc[c] >= 0 else None,
                    'peak_sigma': float(peak_sigma[c]) if peak_sigma[c] >= 0 else None,
                    'peak_location': (
                        {'record': peak_loc[c][0], 'frag': peak_loc[c][1], 'frame': peak_loc[c][2]}
                        if peak_loc[c] else None
                    ),
                    **({'n_thresh': int(n_thresh[c])} if args.thresh is not None else {}),
                }
                for c in ch_list
            ],
        }
        with open(args.json, 'w') as jf:
            json.dump(result, jf, indent=2)
        print(f'\nSaved -> {args.json}')


if __name__ == '__main__':
    main()
