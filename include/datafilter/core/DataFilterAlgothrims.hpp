#ifndef DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_
#define DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_

// ============================================================================
// DataFilterAlgothrims : frame-level ADC threshold filtering for WIBEth TPC.
//
// Responsibilities:
//  - Iterate all fragments in a TriggerRecord
//  - For WIBEth fragments: scan every channel/sample ADC value
//    -> keep the fragment if max ADC >= adc_threshold
//    -> drop the fragment otherwise (log the rejection)
//  - Non-WIBEth fragments (DAPHNE, TA, TC, ...) are kept unconditionally
//  - Rebuild and return a new TriggerRecord from the surviving fragments
//  - Return nullptr if no fragments survive (caller drops the TR entirely)
// ============================================================================

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "daqdataformats/Fragment.hpp"
#include "daqdataformats/FragmentHeader.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeader.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"
#include "logging/Logging.hpp"

namespace dunedaq::datafilter {

using trigger_record_ptr_t = std::unique_ptr<daqdataformats::TriggerRecord>;

// ----------------------------------------------------------------------------
// DataFilterAlgothrims
// ----------------------------------------------------------------------------
struct DataFilterAlgothrims {

  // ADC rejection threshold -- configured via OKS (DataFilter.adc_threshold).
  // A WIBEth fragment is KEPT if any channel/sample has ADC >= this value.
  // Set to 0 to keep all fragments (pass-through behaviour).
  uint16_t adc_threshold{0};

  // To influx only true
  bool enable_histogram{false};

  // Frame-level mode -- configured via OKS (DataFilter.enable_frame_filter).
  // false: keep/drop whole WIBEth fragments; histogram gets one entry per
  //        fragment (its overall max ADC).  This is the default behaviour.
  // true:  rebuild each WIBEth fragment from only the frames whose max ADC is
  //        >= adc_threshold; histogram gets one entry per *frame*.
  bool frame_level_filter{false};

  // Histogram binning: fixed number of bins spanning the 14-bit ADC range
  // [0, 16383]. N_HIST_BINS=16384 gives full resolution (1 bin per exact ADC
  // value, since HIST_BIN_WIDTH = 16384 / N_HIST_BINS = 1) -- lower this to
  // trade resolution for a smaller datafilter_adc_histogram.json if needed.
  static constexpr int N_HIST_BINS = 16384;
  static constexpr uint16_t MAX_ADC_14BIT = 16383;
  static constexpr int HIST_BIN_WIDTH = (MAX_ADC_14BIT + 1) / N_HIST_BINS;

  using histogram_t = std::array<uint32_t, N_HIST_BINS>;

  // Accept/reject max_adc histograms, accumulated since the last
  // take_histograms() call -- same reset-on-read pattern as opmon's
  // "amount_since_last_call" counters. Written from the TR-processing
  // thread, read from the opmon-timer thread, hence the mutex.
  mutable std::mutex m_hist_mtx;
  mutable histogram_t m_accepted_hist{};
  mutable histogram_t m_rejected_hist{};

  // Copies out and resets both histograms; call from generate_opmon_data().
  std::pair<histogram_t, histogram_t> take_histograms() {
    std::lock_guard<std::mutex> lk(m_hist_mtx);
    auto accepted = m_accepted_hist;
    auto rejected = m_rejected_hist;
    m_accepted_hist.fill(0);
    m_rejected_hist.fill(0);
    return {accepted, rejected};
  }

  // --------------------------------------------------------------------------
  // Entry point called by DataFilterReceiver for every incoming TR
  // --------------------------------------------------------------------------
  inline trigger_record_ptr_t
  rebuild_trigger_record(trigger_record_ptr_t &tr) const {

    if (!tr) {
      TLOG_DEBUG(5)
          << "DataFilterAlgothrims::rebuild_trigger_record(): null TR";
      return nullptr;
    }

    const auto &frags = tr->get_fragments_ref();
    if (frags.empty()) {
      TLOG() << "DataFilterAlgothrims: TR has no fragments, dropping";
      return nullptr;
    }
    TLOG() << "DataFilterAlgothrims: processing TR with " << frags.size()
           << " fragments, adc_threshold=" << adc_threshold;

    // Collect trigger metadata from first fragment
    std::uint64_t trig_num{0};
    std::uint64_t trig_ts{0};
    if (frags.at(0)) {
      trig_num = frags.at(0)->get_trigger_number();
      trig_ts = frags.at(0)->get_trigger_timestamp();
    }

    // -- Filtering -----------------------------------------------------------
    // Policy: if the TR contains WIBEth fragments and ALL of them are rejected,
    // drop the entire TR (including non-WIBEth payload fragments).
    // If there are no WIBEth fragments at all, keep the TR as-is.
    //
    // Two granularities, selected by frame_level_filter:
    //   false -> whole fragment kept or dropped (original behaviour)
    //   true  -> fragment rebuilt from only the frames that pass
    auto &mutable_frags = tr->get_fragments_ref();

    std::vector<std::unique_ptr<daqdataformats::Fragment>>
        kept_wibeth; // surviving WIBEth (moved, or rebuilt in frame mode)
    std::vector<std::size_t>
        nonwibeth_idx; // non-WIBEth indices (kept if any WIBEth passes)
    std::size_t n_wibeth = 0;

    for (std::size_t i = 0; i < frags.size(); ++i) {
      const auto &fptr = frags[i];
      if (!fptr) {
        TLOG_DEBUG(5) << "DataFilterAlgothrims: null fragment at index " << i
                      << ", skipping";
        continue;
      }

      const auto ftype = fptr->get_fragment_type();
      if (ftype != daqdataformats::FragmentType::kWIBEth) {
        TLOG_DEBUG(5) << "DataFilterAlgothrims: non-WIBEth fragment"
                      << " source_id=" << fptr->get_element_id()
                      << " fragment_type=" << static_cast<int>(ftype)
                      << " trigger=" << trig_num;
        nonwibeth_idx.push_back(i);
        continue;
      }

      ++n_wibeth;

      if (frame_level_filter) {
        // Frame-level: rebuild with only the surviving frames. Returns nullptr
        // when no frame passes, in which case the fragment is dropped.
        auto new_frag = rebuild_fragment_frames(*fptr);
        if (new_frag) {
          kept_wibeth.push_back(std::move(new_frag));
        } else {
          TLOG() << "DataFilterAlgothrims: all frames rejected, dropping"
                 << " WIBEth fragment source_id=" << fptr->get_element_id()
                 << " trigger=" << trig_num << " (max ADC < " << adc_threshold
                 << ")";
        }
      } else {
        // Fragment-level (original behaviour): keep or drop the whole thing.
        if (passes_adc_threshold(*fptr)) {
          kept_wibeth.push_back(std::move(mutable_frags[i]));
        } else {
          TLOG() << "DataFilterAlgothrims: dropping WIBEth fragment"
                 << " source_id=" << fptr->get_element_id()
                 << " trigger=" << trig_num << " (max ADC < " << adc_threshold
                 << ")";
        }
      }
    }

    // Decision: if there were WIBEth fragments but none passed -> drop whole TR
    if (n_wibeth > 0 && kept_wibeth.empty()) {
      TLOG() << "DataFilterAlgothrims: all " << n_wibeth
             << " WIBEth fragments rejected for trigger=" << trig_num
             << ", dropping TR";
      return nullptr;
    }

    if (kept_wibeth.empty() && nonwibeth_idx.empty()) {
      TLOG() << "DataFilterAlgothrims: no fragments to keep for trigger="
             << trig_num << ", dropping TR";
      return nullptr;
    }

    // -- Rebuild TriggerRecord from kept fragments ---------------------------
    // Order preserved from the original implementation: WIBEth then non-WIBEth.
    auto new_tr =
        std::make_unique<daqdataformats::TriggerRecord>(tr->get_header_ref());

    const std::size_t n_kept = kept_wibeth.size() + nonwibeth_idx.size();
    for (auto &f : kept_wibeth) {
      new_tr->add_fragment(std::move(f));
    }
    for (std::size_t idx : nonwibeth_idx) {
      new_tr->add_fragment(std::move(mutable_frags[idx]));
    }

    TLOG() << "DataFilterAlgothrims: trigger=" << trig_num << " kept " << n_kept
           << "/" << frags.size() << " fragments"
           << (frame_level_filter ? " (frame-level)" : " (fragment-level)");

    return new_tr;
  }

private:
  // -- Max ADC within a single frame -----------------------------------------
  // When early_exit is true this may return as soon as adc_threshold is
  // reached, so the value is then only guaranteed to be >= adc_threshold, not
  // the true maximum. That is fine for a pure keep/drop decision but NOT for
  // histogramming, so callers pass early_exit=false when binning.
  inline uint16_t frame_max_adc(const fddetdataformats::WIBEthFrame &frame,
                                bool early_exit) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    uint16_t max_adc = 0;
    for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
      for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
           ++sample) {
        const uint16_t adc = frame.get_adc(ch, sample);
        if (adc > max_adc)
          max_adc = adc;
        if (early_exit && max_adc >= adc_threshold)
          return max_adc;
      }
    }
    return max_adc;
  }

  // -- Copy the header metadata of one fragment onto another -----------------
  inline void copy_fragment_header(const daqdataformats::Fragment &src,
                                   daqdataformats::Fragment &dst) const {
    dst.set_type(src.get_fragment_type());
    dst.set_trigger_number(src.get_trigger_number());
    dst.set_trigger_timestamp(src.get_trigger_timestamp());
    dst.set_window_begin(src.get_window_begin());
    dst.set_window_end(src.get_window_end());
    dst.set_run_number(src.get_run_number());
    dst.set_element_id(src.get_element_id());
    dst.set_error_bits(src.get_error_bits());
  }

  // -- Frame-level reconstruction inside one WIBEth fragment -----------------
  // Rebuilds the fragment containing only the frames whose max ADC is
  // >= adc_threshold. When enable_histogram is set, also bins one entry per
  // *frame* into the accept/reject histograms (vs one per fragment in the
  // fragment-level path).
  // Returns nullptr if no frame survives, so the caller drops the fragment.
  //
  // Note: n_frames floors, so if the payload is not an exact multiple of
  // sizeof(WIBEthFrame) the trailing partial bytes are not carried over.
  inline std::unique_ptr<daqdataformats::Fragment>
  rebuild_fragment_frames(const daqdataformats::Fragment &frag) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    const auto *payload = static_cast<const uint8_t *>(frag.get_data());
    const std::size_t n_bytes = frag.get_data_size();

    if (n_bytes < sizeof(WIBEthFrame)) {
      // Too small to hold even one frame -- keep the payload unfiltered rather
      // than discarding data we cannot parse (matches the fragment-level path).
      std::vector<std::pair<void *, std::size_t>> whole = {
          {const_cast<void *>(frag.get_data()), n_bytes}};
      auto copy = std::make_unique<daqdataformats::Fragment>(whole);
      copy_fragment_header(frag, *copy);
      TLOG() << "DataFilterAlgothrims: WIBEth fragment payload too small ("
             << n_bytes << " B < " << sizeof(WIBEthFrame)
             << " B), kept unfiltered";
      return copy;
    }

    const std::size_t n_frames = n_bytes / sizeof(WIBEthFrame);

    std::vector<std::pair<void *, std::size_t>> pieces;
    pieces.reserve(n_frames);

    for (std::size_t fi = 0; fi < n_frames; ++fi) {
      const auto *frame = reinterpret_cast<const WIBEthFrame *>(
          payload + fi * sizeof(WIBEthFrame));

      // Need the true max when histogramming; otherwise we can early-exit.
      const uint16_t max_adc = frame_max_adc(*frame, !enable_histogram);
      const bool passed = max_adc >= adc_threshold;

      if (enable_histogram) {
        const int bin =
            std::min(static_cast<int>(max_adc) / HIST_BIN_WIDTH,
                     N_HIST_BINS - 1);
        std::lock_guard<std::mutex> lk(m_hist_mtx);
        (passed ? m_accepted_hist : m_rejected_hist)[bin]++;
      }

      if (passed) {
        pieces.emplace_back(
            const_cast<void *>(static_cast<const void *>(frame)),
            sizeof(WIBEthFrame));
      }
    }

    if (pieces.empty())
      return nullptr;

    // Fragment(pieces) allocates a new buffer, copies the selected frames
    // contiguously after the header, and sets the size field automatically.
    auto new_frag = std::make_unique<daqdataformats::Fragment>(pieces);
    copy_fragment_header(frag, *new_frag);

    TLOG_DEBUG(5) << "DataFilterAlgothrims: rebuild_fragment_frames kept "
                  << pieces.size() << "/" << n_frames << " frames"
                  << " source_id=" << frag.get_element_id();
    return new_frag;
  }

  // -- ADC threshold check for one WIBEth fragment ---------------------------
  // Returns true if the fragment has at least one ADC sample >=
  // adc_threshold.
  inline bool passes_adc_threshold(const daqdataformats::Fragment &frag) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    const auto *payload = static_cast<const uint8_t *>(frag.get_data());
    const std::size_t n_bytes = frag.get_data_size();

    if (n_bytes < sizeof(WIBEthFrame)) {
      TLOG() << "DataFilterAlgothrims: WIBEth fragment payload too small ("
             << n_bytes << " B < " << sizeof(WIBEthFrame)
             << " B), keeping unconditionally";
      return true;
    }

    const std::size_t n_frames = n_bytes / sizeof(WIBEthFrame);
    uint16_t max_adc = 0;

    if (!enable_histogram) {
      // Original fast path: early-exit as soon as the threshold is crossed.
      // No histogram bookkeeping -- this is the only work done when the
      // opmon-to-influx feature is disabled (the default).
      for (std::size_t fi = 0; fi < n_frames; ++fi) {
        const auto *frame = reinterpret_cast<const WIBEthFrame *>(
            payload + fi * sizeof(WIBEthFrame));

        for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
          for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
               ++sample) {
            const uint16_t adc = frame->get_adc(ch, sample);
            if (adc > max_adc)
              max_adc = adc;
            if (max_adc >= adc_threshold)
              return true; // early exit
          }
        }
      }

      TLOG_DEBUG(5) << "DataFilterAlgothrims: fragment max_adc=" << max_adc
                    << " < threshold=" << adc_threshold;
      return false;
    }

    // Histogram path: full scan, no early exit -- needs the true max_adc,
    // not just whether the threshold was crossed.
    for (std::size_t fi = 0; fi < n_frames; ++fi) {
      const auto *frame = reinterpret_cast<const WIBEthFrame *>(
          payload + fi * sizeof(WIBEthFrame));

      for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
        for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
             ++sample) {
          const uint16_t adc = frame->get_adc(ch, sample);
          if (adc > max_adc)
            max_adc = adc;
        }
      }
    }

    const bool passed = max_adc >= adc_threshold;
    const int bin =
        std::min(static_cast<int>(max_adc) / HIST_BIN_WIDTH, N_HIST_BINS - 1);
    {
      std::lock_guard<std::mutex> lk(m_hist_mtx);
      (passed ? m_accepted_hist : m_rejected_hist)[bin]++;
    }

    if (!passed) {
      TLOG_DEBUG(5) << "DataFilterAlgothrims: fragment max_adc=" << max_adc
                    << " < threshold=" << adc_threshold;
    }
    return passed;
  }
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_
