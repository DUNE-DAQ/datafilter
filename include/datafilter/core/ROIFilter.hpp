#ifndef DATAFILTER_CORE_ROIFILTER_HPP_
#define DATAFILTER_CORE_ROIFILTER_HPP_

#include <algorithm>
#include <cstdint>
#include <map>
#include <vector>

#include "daqdataformats/Fragment.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"

namespace dunedaq::datafilter {

// Ported from https://github.com/DUNE/duneana/tree/develop/duneana/2DROIAna
// (ROIAna_module.cc/ROIProd_module.cc, 2026.10.09 version):
// per-channel median + peak-deviation region-of-interest detector,
// not over a full LArSoft readout window
// like the original -- see ProcessROIWide() (per WIBEth fragment)
// and ProcessROIFrame() (per single frame).
struct ROIFilter {
  uint16_t roi_peak_threshold{0}; // duneana: fROI_Peak
  uint16_t roi_channel_window{0}; // duneana: fROI_CH

  // Shared by ProcessROIWide/ProcessROIFrame: flags `channel` (dilating
  // +/-roi_channel_window, clamped to [0, s_num_channels-1]) in `ret` if
  // `charges`' max/min deviation from their own median exceeds
  // roi_peak_threshold.
  inline void flag_if_peak(int channel, const std::vector<float> &charges,
                           std::map<int, bool> &ret) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    auto chargessort = charges;
    std::nth_element(chargessort.begin(),
                     chargessort.begin() + chargessort.size() / 2,
                     chargessort.end());
    const float median = chargessort[chargessort.size() / 2];

    const float adc_max =
        *std::max_element(charges.begin(), charges.end()) - median;
    const float adc_min =
        *std::min_element(charges.begin(), charges.end()) - median;

    if (adc_min < -static_cast<float>(roi_peak_threshold) ||
        adc_max > static_cast<float>(roi_peak_threshold)) {
      const int lo =
          std::max(0, channel - static_cast<int>(roi_channel_window));
      const int hi = std::min(static_cast<int>(WIBEthFrame::s_num_channels) - 1,
                              channel + static_cast<int>(roi_channel_window));
      for (int c = lo; c <= hi; ++c) {
        ret[c] = true;
      }
    }
  }

  // Ported from ProcessROIWide (ROIAna_module.cc:152-197 / ROIProd_module.cc:
  // 177-222): for each of a fragment's 64 channels, median across all
  // samples in the fragment (every frame), then flag_if_peak() against that
  // median -- see ProcessROIFrame() for the per-frame variant.
  inline std::map<int, bool>
  ProcessROIWide(const daqdataformats::Fragment &frag) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    std::map<int, bool> ret;
    for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
      ret[ch] = false;
    }

    const auto *payload = static_cast<const uint8_t *>(frag.get_data());
    const std::size_t n_bytes = frag.get_data_size();
    if (n_bytes < sizeof(WIBEthFrame)) {
      return ret;
    }
    const std::size_t n_frames = n_bytes / sizeof(WIBEthFrame);

    for (int channel = 0; channel < WIBEthFrame::s_num_channels; ++channel) {
      std::vector<float> charges;
      charges.reserve(n_frames * WIBEthFrame::s_time_samples_per_frame);
      for (std::size_t fi = 0; fi < n_frames; ++fi) {
        const auto *frame = reinterpret_cast<const WIBEthFrame *>(
            payload + fi * sizeof(WIBEthFrame));
        for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
             ++sample) {
          charges.push_back(frame->get_adc(channel, sample));
        }
      }

      flag_if_peak(channel, charges, ret);
    }

    return ret;
  }

  // Per-frame ROI decision: like ProcessROIWide, but the median/deviation
  // for each channel is computed over just this one frame's 64 samples,
  // not every frame in the fragment. Used by frame-level ROI filtering.
  inline std::map<int, bool>
  ProcessROIFrame(const fddetdataformats::WIBEthFrame &frame) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    std::map<int, bool> ret;
    for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
      ret[ch] = false;
    }

    for (int channel = 0; channel < WIBEthFrame::s_num_channels; ++channel) {
      std::vector<float> charges;
      charges.reserve(WIBEthFrame::s_time_samples_per_frame);
      for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
           ++sample) {
        charges.push_back(frame.get_adc(channel, sample));
      }

      flag_if_peak(channel, charges, ret);
    }

    return ret;
  }

  // Ported from ROIProd::ROIFilter (ROIProd_module.cc:403-430) -- zeros the
  // ADC samples of every channel not flagged in `ret`, in place. Named
  // apply() rather than ROIFilter() since a method can't share its
  // enclosing struct's exact name.
  inline void apply(daqdataformats::Fragment &frag,
                    const std::map<int, bool> &ret) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    auto *payload = static_cast<uint8_t *>(frag.get_data());
    const std::size_t n_bytes = frag.get_data_size();
    if (n_bytes < sizeof(WIBEthFrame)) {
      return;
    }
    const std::size_t n_frames = n_bytes / sizeof(WIBEthFrame);

    for (std::size_t fi = 0; fi < n_frames; ++fi) {
      auto *frame =
          reinterpret_cast<WIBEthFrame *>(payload + fi * sizeof(WIBEthFrame));
      for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
        const auto it = ret.find(ch);
        const bool in_roi = (it != ret.end()) && it->second;
        if (!in_roi) {
          for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
               ++sample) {
            frame->set_adc(ch, sample, 0);
          }
        }
      }
    }
  }
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_CORE_ROIFILTER_HPP_
