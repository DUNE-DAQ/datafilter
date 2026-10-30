"""
plot_frame_filter.py -- visualize DataFilterAlgothrims's frame-level ADC
filtering (enable_frame_filter=true) as a time-window filter.

There is no separate "time window" parameter in DataFilterAlgothrims.hpp --
"frame-level filtering" IS the time-windowing: WIBEth data is laid out as
fixed-size 7200-byte frames (64 samples x 64 channels each), and
enable_frame_filter=true makes the ADC-threshold decision independently for
each frame instead of for the whole fragment. This script mirrors that exact
decision (include/datafilter/core/DataFilterAlgothrims.hpp,
frame_max_adc()/rebuild_fragment_frames()) in plain numpy, with no C++
involved, and plots which time windows of a real fragment would be kept vs
dropped.

Data access follows scan_np04hd.py's approach (plain h5py + vectorized numpy
unpacking, no hdf5libs/daqdataformats python bindings required). Plotting
follows view_adc.py's approach (matplotlib, Agg backend, --save to PNG).

IMPORTANT: run this against a RAW (pre-filter) HDF5 file, e.g. the
input_h5_filename under storage_pathname in test/config/dfSession.data.xml,
or the NP04 coldbox file scan_np04hd.py has been run against. A
FilterResultWriter *output* file has already had every rejected frame
removed, so every frame in it would show as "kept" and the plot would be
empty of anything interesting.

Usage:
  python3 plot_frame_filter.py <file.hdf5> [--thresh 9130] [--record NAME]
                                [--frag NAME] [--channel SPEC] [--save out.png]

--channel accepts a single index (5), a range (0-7), or a comma list mixing
both (0-3,10,20-22); each selected channel is overlaid as its own line on
the waveform panel. The frame kept/dropped decision always uses the max over
all 64 channels (matching frame_max_adc()), regardless of which subset of
channels is plotted.
"""

import argparse
import os

import h5py
import numpy as np
import matplotlib
matplotlib.use('QtAgg')  # headless; swap for 'TkAgg' if you have a display and Tk installed
import matplotlib.pyplot as plt

# WIBEth frame layout constants -- match scan_np04hd.py exactly.
SAMPLES, CHANNELS, BITS = 64, 64, 14
N_WORDS = (CHANNELS * BITS + 63) // 64          # = 14
FRAME_HDR = 32                                  # 16 B DAQEthHeader + 16 B WIBEthHeader
FRAME_SIZE = FRAME_HDR + SAMPLES * N_WORDS * 8  # = 7200, sizeof(WIBEthFrame)
FRAG_HDR = 72                                   # raw Fragment header size before frame data starts
MASK14 = 0x3FFF

FRAME_WORDS = FRAME_SIZE // 8   # 900 uint64 words per frame
HDR_WORDS = FRAME_HDR // 8      # 4 header words to skip per frame

# Word 1 of DAQEthHeader is the full 64-bit `timestamp` field, hence
# TIMESTAMP_WORD = 1.
TIMESTAMP_WORD = 1

# DUNE DAQ timing-system clock: 62.5 MHz, i.e. 1 tick = 16 ns. Confirmed via
# confmodel/schema/confmodel/dunedaq.schema.xml's clock_speed_hz and matching
# literals across fdreadoutlibs frame processors and timinglibs.
CLOCK_FREQUENCY_HZ = 62_500_000

# Precomputed per-channel extraction tables (constant for all WIBEth frames)
_CH = np.arange(CHANNELS, dtype=np.uint64)
_WORD_IDX = ((np.uint64(BITS) * _CH) // np.uint64(64)).astype(np.intp)
_BIT_OFF = ((np.uint64(BITS) * _CH) % np.uint64(64))
_SPANS = (_BIT_OFF + np.uint64(BITS)) > np.uint64(64)
_MASK14 = np.uint64(MASK14)


def unpack_frames(payload):
    """Vectorized decode of a WIBEth fragment payload (post FRAG_HDR strip).

    Returns (adcs, timestamps):
      adcs       -- (n_frames, SAMPLES, CHANNELS) uint16, ADC value per
                    (frame, sample, channel). Axis order confirmed against
                    WIBEthFrame.hpp's adc_words[sample][channel-word] layout
                    and scan_np04hd.py's own reshape -- sample-major, NOT
                    channel-major.
      timestamps -- (n_frames,) uint64, each frame's DAQEthHeader.timestamp.

    Returns (None, None) if payload holds no complete frame.
    """
    n = len(payload) // FRAME_SIZE
    if n == 0:
        return None, None

    words = np.frombuffer(payload[:n * FRAME_SIZE], dtype='<u8').reshape(n, FRAME_WORDS)
    timestamps = words[:, TIMESTAMP_WORD].copy()

    data = words[:, HDR_WORDS:].reshape(n, SAMPLES, N_WORDS)
    adcs = (data[:, :, _WORD_IDX] >> _BIT_OFF) & _MASK14
    if _SPANS.any():
        sl = _BIT_OFF[_SPANS]
        adcs[:, :, _SPANS] |= (data[:, :, _WORD_IDX[_SPANS] + 1] << (np.uint64(64) - sl)) & _MASK14

    return adcs.astype(np.uint16), timestamps


def roi_frame_decision(adcs, roi_peak_threshold, roi_channel_window):
    """Vectorized mirror of ROIFilter::ProcessROIFrame + flag_if_peak
    (include/datafilter/core/ROIFilter.hpp), used when enable_roi_filter is
    also set. Per frame, per channel: median via nth_element-equivalent
    (sorted index SAMPLES/2, not an averaged median), flag if the max/min
    deviation from that median exceeds roi_peak_threshold, then dilate the
    flag +/-roi_channel_window across channels (clamped to [0,63]).
    A frame is kept if any_of(channel map) is true -- dilation only adds
    true entries, so this equals "did any channel's own deviation cross
    roi_peak_threshold" (no need to re-derive it after dilation).

    Returns (kept, dev_mag_per_frame): kept is (n_frames,) bool; dev_mag_per_frame
    is (n_frames,) float, the max per-channel deviation magnitude pre-dilation
    (plotted against roi_peak_threshold).
    """
    n_ch = adcs.shape[2]
    sorted_adcs = np.sort(adcs.astype(np.float32), axis=1)
    median = sorted_adcs[:, sorted_adcs.shape[1] // 2, :]
    adc_max = sorted_adcs[:, -1, :] - median
    adc_min = sorted_adcs[:, 0, :] - median
    primary = (adc_min < -float(roi_peak_threshold)) | (adc_max > float(roi_peak_threshold))

    flagged = primary.copy()
    for c in range(n_ch):
        lo, hi = max(0, c - roi_channel_window), min(n_ch - 1, c + roi_channel_window)
        flagged[:, c] = primary[:, lo:hi + 1].any(axis=1)

    dev_mag = np.maximum(adc_max, -adc_min)
    return flagged.any(axis=1), dev_mag.max(axis=1)


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


def find_default_record_and_frag(f):
    """First TriggerRecord* group and first WIBEth dataset within it."""
    for tr_name in sorted(f.keys()):
        if not tr_name.startswith('TriggerRecord'):
            continue
        rdata_path = tr_name + '/RawData'
        if rdata_path not in f:
            continue
        for ds in f[rdata_path]:
            if 'WIBEth' in ds:
                return tr_name, ds
    return None, None


def main():
    ap = argparse.ArgumentParser(
        description='Plot frame-level ADC filtering (time-window view) for one WIBEth fragment')
    ap.add_argument('fname', help='HDF5 file to read (RAW/pre-filter file, not FRW output)')
    ap.add_argument('--thresh', type=int, default=9130,
                     help='ADC threshold; matches OKS adc_threshold (default: 9130)')
    ap.add_argument('--roi', action='store_true',
                     help='Use ROIFilter::ProcessROIFrame (median/peak-deviation) instead of '
                          'the flat --thresh rule; matches enable_roi_filter && enable_frame_filter')
    ap.add_argument('--roi-peak', type=int, default=100,
                     help='roi_peak_threshold; matches OKS attr (default: 100)')
    ap.add_argument('--roi-window', type=int, default=10,
                     help='roi_channel_window; matches OKS attr (default: 10)')
    ap.add_argument('--record', default=None,
                     help='TriggerRecord group name (default: first found)')
    ap.add_argument('--frag', default=None,
                     help='WIBEth dataset name within the record (default: first found)')
    ap.add_argument('--channel', default=None,
                     help='Channel(s) to overlay: a single index (5), a range (0-7), or a '
                          'comma list mixing both (0-3,10,20-22). Default: per-sample max '
                          'across all 64 channels, matching what frame_max_adc() evaluates')
    ap.add_argument('--save', default=None,
                     help='Output PNG path (default: frame_filter_<record>_<frag>.png)')
    args = ap.parse_args()

    f = h5py.File(args.fname, 'r')

    record, frag_name = args.record, args.frag
    if record is None or frag_name is None:
        auto_record, auto_frag = find_default_record_and_frag(f)
        record = record or auto_record
        frag_name = frag_name or auto_frag
    if record is None or frag_name is None:
        f.close()
        raise SystemExit('No TriggerRecord group with a WIBEth fragment found in this file')

    ds_path = f'{record}/RawData/{frag_name}'
    if ds_path not in f:
        f.close()
        raise SystemExit(f'{ds_path} not found in {args.fname}')
    raw = bytes(f[ds_path][()])
    f.close()

    if len(raw) <= FRAG_HDR:
        raise SystemExit(f'{ds_path}: {len(raw)} B, no payload beyond the {FRAG_HDR} B fragment header')
    payload = raw[FRAG_HDR:]

    if len(payload) < FRAME_SIZE:
        # Mirrors DataFilterAlgothrims.hpp:263-274 exactly: a payload smaller
        # than one frame is kept unconditionally, unfiltered. Nothing to plot.
        print(f'{ds_path}: payload {len(payload)} B < {FRAME_SIZE} B (one frame) -- '
              f'the real filter keeps this unfiltered (DataFilterAlgothrims.hpp:263-274). '
              f'Nothing to plot.')
        return

    adcs, timestamps = unpack_frames(payload)
    n_frames = adcs.shape[0]

    if args.roi:
        kept, per_frame_stat = roi_frame_decision(adcs, args.roi_peak, args.roi_window)
        bottom_thresh = args.roi_peak
        bottom_ylabel = 'max |dev from per-channel median| (ROI)'
        mode_label = f'roi_peak={args.roi_peak}  roi_window={args.roi_window}'
    else:
        # frame_max_adc(): max over all channels and samples within each frame.
        per_frame_stat = adcs.max(axis=(1, 2))
        # rebuild_fragment_frames()'s comparison is >= , not > (:287).
        kept = per_frame_stat >= args.thresh
        bottom_thresh = args.thresh
        bottom_ylabel = 'frame_max_adc'
        mode_label = f'thresh={args.thresh}'

    # Self-verify the timestamp decode: consecutive frames must be a constant
    # tick delta apart. If not, TIMESTAMP_WORD is wrong (or this fragment has
    # a genuine timing discontinuity) -- fall back rather than mislabel.
    have_time_axis = False
    tick_delta = None
    if n_frames > 1:
        deltas = np.diff(timestamps.astype(np.int64))
        if np.all(deltas == deltas[0]) and deltas[0] != 0:
            have_time_axis = True
            tick_delta = int(deltas[0])
        else:
            uniq = np.unique(deltas)[:5]
            print(f'WARNING: per-frame timestamp deltas are not constant '
                  f'(seen: {uniq}{"..." if len(np.unique(deltas)) > 5 else ""}); '
                  f'falling back to sample-index-only labeling.')

    ch_list = None
    if args.channel is not None:
        ch_list = parse_channel_spec(args.channel, CHANNELS)
        wf_label = f'ch{ch_list[0]}' if len(ch_list) == 1 else f'{len(ch_list)} channels ({args.channel})'
    else:
        waveform = adcs.max(axis=2).reshape(-1)  # per-sample max across channels
        wf_label = 'max over 64 channels'

    n_kept, n_dropped = int(kept.sum()), int((~kept).sum())
    title = (f'{os.path.basename(args.fname)}  {record}  {frag_name}\n'
             f'{mode_label}  frames kept={n_kept}/{n_frames}  dropped={n_dropped}/{n_frames}')

    fig, (ax_wave, ax_frame) = plt.subplots(
        2, 1, figsize=(14, 7), sharex=True,
        gridspec_kw={'height_ratios': [2, 1]})

    # -- top panel: waveform, shaded per frame by its kept/dropped decision --
    # roi_peak_threshold is a per-frame per-channel deviation, not an absolute
    # ADC level, so no threshold line belongs on this raw-ADC panel in ROI mode.
    n_samples_total = n_frames * SAMPLES
    x_full = np.arange(n_samples_total)
    thresh_line = None if args.roi else ax_wave.axhline(args.thresh, color='gray', ls='--', lw=1)
    if ch_list is not None:
        for c in ch_list:
            ax_wave.plot(x_full, adcs[:, :, c].reshape(-1), lw=0.5, alpha=0.7, label=f'ch{c}')
        handles, labels = ax_wave.get_legend_handles_labels()
        if thresh_line is not None:
            handles, labels = handles + [thresh_line], labels + [f'thresh={args.thresh}']
        if len(ch_list) <= 8 or thresh_line is not None:
            ax_wave.legend(handles, labels, loc='upper right', fontsize=7)
        ax_wave.set_ylabel('ADC (14-bit)')
    else:
        ax_wave.plot(x_full, waveform, lw=0.6, color='black')
        if thresh_line is not None:
            ax_wave.legend([thresh_line], [f'thresh={args.thresh}'], loc='upper right', fontsize=8)
        ax_wave.set_ylabel(f'ADC ({wf_label})')
    for fi in range(n_frames):
        color = 'tab:green' if kept[fi] else 'tab:red'
        ax_wave.axvspan(fi * SAMPLES, (fi + 1) * SAMPLES, color=color, alpha=0.12, lw=0)
    ax_wave.tick_params(labelbottom=False)

    # -- bottom panel: the decision statistic per frame, same x-axis (sample
    #    index) as the top panel, so bars line up exactly under their axvspan --
    colors = ['tab:green' if k else 'tab:red' for k in kept]
    ax_frame.bar(np.arange(n_frames) * SAMPLES, per_frame_stat, width=SAMPLES,
                 align='edge', color=colors, edgecolor='none')
    ax_frame.axhline(bottom_thresh, color='gray', ls='--', lw=1)

    tick_stride = max(1, n_frames // 12)
    tick_frames = np.arange(0, n_frames, tick_stride)
    tick_pos = tick_frames * SAMPLES + SAMPLES / 2
    if have_time_axis:
        tick_labels = [
            f'{fi}\n(+{(timestamps[fi] - timestamps[0]) / CLOCK_FREQUENCY_HZ * 1e6:.1f} us)'
            for fi in tick_frames
        ]
    else:
        tick_labels = [str(fi) for fi in tick_frames]
    ax_frame.set_xticks(tick_pos)
    ax_frame.set_xticklabels(tick_labels, fontsize=7)
    ax_frame.set_xlim(0, n_samples_total)
    ax_frame.set_xlabel('Frame index' +
                        (' (with relative DTS tick offset)' if have_time_axis else
                         ' -- sample index axis only; timestamp decode unconfirmed'))
    ax_frame.set_ylabel(bottom_ylabel)

    fig.suptitle(title)
    plt.tight_layout()

    save_path = args.save or f'frame_filter_{record}_{frag_name}.png'
    fig.savefig(save_path, dpi=150)
    plt.show()  # no-op under Agg; opens a window under an interactive backend
    plt.close(fig)

    print(f'Saved -> {save_path}')
    print(f'{n_frames} frames, {n_kept} kept / {n_dropped} dropped ({mode_label})')
    if have_time_axis:
        print(f'Per-frame tick delta: {tick_delta} ticks '
              f'({tick_delta / CLOCK_FREQUENCY_HZ * 1e6:.2f} us) -- '
              f'constant across all {n_frames - 1} consecutive pairs')


if __name__ == '__main__':
    main()
