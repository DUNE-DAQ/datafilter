#ifndef DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_
#define DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_

#include <algorithm>
#include <atomic>
#include <cctype> // std::tolower
#include <condition_variable>
#include <execution>
#include <fstream>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "datafilter/datafilter_structs.hpp"
#include "iomanager/IOManager.hpp"

using namespace dunedaq::iomanager;

namespace dunedaq {
namespace datafilter {

struct RunInfo {
  std::atomic<unsigned int> run_number{0};
  std::atomic<unsigned int> file_index{0};

  mutable std::mutex mutex; // mutable allows const methods to lock

  // Set the run information
  void set(unsigned int run, unsigned int file_idx) {
    std::lock_guard<std::mutex> lock(mutex);
    run_number = run;
    file_index = file_idx;
  }

  // Get the run information
  std::pair<unsigned int, unsigned int> get() const {
    std::lock_guard<std::mutex> lock(mutex);
    return {run_number.load(), file_index.load()};
  }
};

struct BookkeepingReceiver {
  RunInfo &run_info;

  // Thread control
  std::atomic<bool> stop_flag{false};
  std::unique_ptr<std::thread> receiver_thread;
  std::unique_ptr<std::thread> writer_thread;

  // Each queue entry carries its target filename alongside the JSON entry,
  // enabling per-cycle file routing without shared mutable filename state.
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  std::queue<std::pair<std::string, nlohmann::json>> bk_queue;

  // Transfer rate tracking
  std::atomic<double> transfer_rate_mbps{0};
  std::mutex rate_mutex;

  std::string m_bk_rx_uid;
  std::string m_bk_tx_uid;
  std::string m_bk_trd_uid;
  std::string m_session_name;
  std::string datafilter_id;
  mutable std::mutex id_mutex;

  // Per-cycle BK state machine, keyed by df_cycle_id minted on each
  // kAssignedToFilter.  Replaces the single received_cnt / all_bk_received
  // that could not handle concurrent cycles.
  struct CycleState {
    bool frw_confirmed = false;
    bool trd_final = false;
    std::string bk_filename;
    unsigned int run_number = 0;
    std::vector<uint64_t>
        filtered_triggers; // trigger numbers dropped by DataFilter
  };
  std::unordered_map<uint64_t, CycleState> m_cycles;
  std::mutex m_cycles_mtx;
  std::atomic<uint64_t> m_next_cycle_id{0};
  // Counts open cycles; stop() waits until this reaches 0.
  std::atomic<int> m_active_cycles{0};
  std::mutex m_all_done_mtx;
  std::condition_variable m_all_done_cv;

  std::atomic<bool> callback_registered{false};

  std::atomic<double> transfer_rate_in_mbps{0.0};
  std::atomic<double> transfer_rate_out_mbps{0.0};

  std::shared_ptr<
      dunedaq::iomanager::SenderConcept<dunedaq::datafilter::BookKeeping>>
      m_bk_sender;

  explicit BookkeepingReceiver(RunInfo &info, std::string id = "",
                               std::string bk_rx_uid = "",
                               std::string bk_tx_uid = "",
                               std::string session_name = "",
                               std::string bk_trd_uid = "")
      : run_info(info), datafilter_id(std::move(id)),
        m_bk_rx_uid(std::move(bk_rx_uid)), m_bk_tx_uid(std::move(bk_tx_uid)),
        m_session_name(std::move(session_name)),
        m_bk_trd_uid(std::move(bk_trd_uid)) {
    TLOG() << "BookkeepingReceiver initialized";
  }

  ~BookkeepingReceiver() {
    try {
      stop();
    } catch (...) {
    }
    TLOG() << "BookkeepingReceiver destroyed";
  }

  void start() {
    std::lock_guard<std::mutex> lock(queue_mutex);
    if (receiver_thread) {
      TLOG() << "Receiver already running";
      return;
    }
    stop_flag.store(false);
    writer_thread = std::make_unique<std::thread>([this]() {
      TLOG() << "Starting writer thread (ID: " << std::this_thread::get_id()
             << ")";
      this->write_to_file();
    });
    receiver_thread = std::make_unique<std::thread>([this]() {
      TLOG() << "Starting receiver thread (ID: " << std::this_thread::get_id()
             << ")";
      this->receive_bk();
      TLOG() << "Receiver thread exiting";
    });
    TLOG() << "Bookkeeping receiver started";
  }

  void stop() {
    TLOG() << "Initiating bookkeeping receiver shutdown";
    {
      std::unique_lock<std::mutex> lk(m_all_done_mtx);
      bool done = m_all_done_cv.wait_for(lk, std::chrono::minutes(10), [this] {
        return m_active_cycles.load(std::memory_order_acquire) == 0;
      });
      if (!done)
        TLOG() << "stop(): timed out waiting for all cycles — stopping anyway";
      else
        TLOG() << "stop(): all cycles complete, draining queue";
    }
    stop_flag.store(true);
    callback_registered.store(false, std::memory_order_relaxed);
    queue_cv.notify_all();

    if (receiver_thread && receiver_thread->joinable())
      receiver_thread->join();
    receiver_thread.reset();

    if (writer_thread && writer_thread->joinable())
      writer_thread->join();
    writer_thread.reset();

    TLOG() << "Bookkeeping receiver fully stopped";
  }

  void set_transfer_rate(double transfer_rate) {
    std::lock_guard<std::mutex> lock(rate_mutex);
    transfer_rate_mbps = transfer_rate;
  }

  double get_transfer_rate() {
    std::lock_guard<std::mutex> lock(rate_mutex);
    return transfer_rate_mbps.load();
  }

  void set_transfer_rate_in(double mbps) {
    transfer_rate_in_mbps.store(mbps, std::memory_order_relaxed);
  }
  void set_transfer_rate_out(double mbps) {
    transfer_rate_out_mbps.store(mbps, std::memory_order_relaxed);
  }
  double get_transfer_rate_in() const {
    return transfer_rate_in_mbps.load(std::memory_order_relaxed);
  }
  double get_transfer_rate_out() const {
    return transfer_rate_out_mbps.load(std::memory_order_relaxed);
  }

  // Called by DataFilterReceiver when a TR is dropped by the filter algorithm.
  // Records the trigger number so it appears in the bookkeeping JSON alongside
  // the written TRs for the same cycle.
  void record_filtered_trigger(uint64_t trig_num) {
    std::lock_guard<std::mutex> lk(m_cycles_mtx);
    // With serialized dispatch there is at most one active cycle.
    for (auto &[id, cs] : m_cycles)
      if (!cs.frw_confirmed)
        cs.filtered_triggers.push_back(trig_num);
  }

  std::string get_datafilter_id() const {
    std::lock_guard<std::mutex> lock(id_mutex);
    return datafilter_id;
  }

  static inline std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return s;
  }

private:
  // Forward FRW's completion BK to TRD via bookkeeping2.
  // Copies file_attributes_info so that df_cycle_id and trd_bk_seq propagate
  // to TRD's always-on callback for per-cycle waiter routing.
  void send_completion_to_trd(const dunedaq::datafilter::BookKeeping &frw_bk) {
    if (m_bk_trd_uid.empty()) {
      TLOG() << "send_completion_to_trd: no TRD uid configured, skipping";
      return;
    }
    dunedaq::datafilter::BookKeeping fwd(m_bk_trd_uid);
    fwd.from_id = "FilterResultWriter";
    fwd.entry_id = frw_bk.entry_id; // propagate FRW completion timestamp
    fwd.run_number = frw_bk.run_number;
    fwd.tr_status = (frw_bk.tr_status == to_string(TRStatus::kFileCompleted))
                        ? to_string(TRStatus::kReRecorded)
                        : to_string(TRStatus::kWriteFailed);
    fwd.tr_header_info = frw_bk.tr_header_info;
    fwd.file_attributes_info = frw_bk.file_attributes_info;

    auto sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(m_bk_trd_uid);
    if (!sender) {
      TLOG() << "send_completion_to_trd: failed to get sender for "
             << m_bk_trd_uid;
      return;
    }
    try {
      sender->send(std::move(fwd), std::chrono::milliseconds(2000));
      TLOG() << "BookkeepingReceiver: forwarded FRW completion ("
             << frw_bk.tr_status << " -> " << fwd.tr_status << ") to TRD";
    } catch (const std::exception &e) {
      TLOG() << "send_completion_to_trd failed: " << e.what();
    }
  }

  void send_bk(dunedaq::datafilter::BookKeeping bk_info) {
    if (m_bk_tx_uid.empty()) {
      TLOG() << "send_bk: m_bk_tx_uid not configured — skipping";
      return;
    }
    m_bk_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(m_bk_tx_uid);
    if (!m_bk_sender) {
      TLOG() << "Failed to get bookkeeping sender!";
      return;
    }
    try {
      m_bk_sender->send(std::move(bk_info), std::chrono::milliseconds(2000));
      TLOG() << "Successfully sent BookKeeping data.";
    } catch (const std::exception &e) {
      TLOG() << "Send failed (non-blocking, continuing): " << e.what();
    }
  }

  void receive_bk() {
    TLOG() << "Setting up bookkeeping receiver";

    if (m_bk_rx_uid.empty()) {
      TLOG() << "receive_bk: m_bk_rx_uid not configured — skipping";
      return;
    }
    auto cb_receiver =
        dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
            m_bk_rx_uid);
    if (!cb_receiver) {
      TLOG() << "Failed to get bookkeeping receiver";
      return;
    }

    auto str_receiver_cb = [&](dunedaq::datafilter::BookKeeping bk) {
      if (stop_flag)
        return;

      const std::string from = to_lower(bk.from_id);
      const bool is_from_trdisp =
          (from.find("trdispatcher") != std::string::npos);
      const bool is_from_writer =
          (from.find("filterresultwriter") != std::string::npos);

      // Helper: extract uint64_t from file_attributes_info by key.
      auto get_attr_u64 = [&](const std::string &key) -> uint64_t {
        for (const auto &kv : bk.file_attributes_info)
          if (kv.first == key)
            try {
              return std::stoull(kv.second);
            } catch (...) {
            }
        return UINT64_MAX;
      };

      if (is_from_trdisp &&
          bk.tr_status == to_string(TRStatus::kAssignedToFilter)) {
        // New pipeline cycle: mint a df_cycle_id and inject into the BK
        // before forwarding to FRW.
        const uint64_t cycle_id = m_next_cycle_id.fetch_add(1);
        bk.file_attributes_info.push_back(
            {"df_cycle_id", std::to_string(cycle_id)});

        std::string file_index_str = "0";
        for (const auto &kv : bk.file_attributes_info)
          if (kv.first == "file_index") {
            file_index_str = kv.second;
            break;
          }
        int file_idx = 0;
        try {
          file_idx = std::stoi(file_index_str);
        } catch (...) {
        }

        run_info.set(bk.run_number, file_idx);
        const std::string fname = generate_bk_filename(bk.run_number, file_idx);

        {
          std::lock_guard<std::mutex> lk(m_cycles_mtx);
          CycleState &cs = m_cycles[cycle_id];
          cs.run_number = bk.run_number;
          cs.bk_filename = fname;
        }
        m_active_cycles.fetch_add(1, std::memory_order_relaxed);

        bk.transfer_rate = get_transfer_rate_in();
        bk.datafilter_id = get_datafilter_id();

        {
          std::lock_guard<std::mutex> lk(queue_mutex);
          bk_queue.emplace(fname, to_json(bk));
        }
        queue_cv.notify_one();

        if (!m_bk_tx_uid.empty()) {
          try {
            send_bk(bk);
          } catch (const std::exception &e) {
            TLOG() << "send_bk failed (non-critical): " << e.what();
          }
        }

        TLOG() << "New cycle " << cycle_id << " (run=" << bk.run_number
               << " file=" << file_idx << ")";
        return;
      }

      // All other messages carry df_cycle_id in file_attributes_info.
      const uint64_t cycle_id = get_attr_u64("df_cycle_id");

      std::string fname;
      if (cycle_id != UINT64_MAX) {
        std::lock_guard<std::mutex> lk(m_cycles_mtx);
        auto it = m_cycles.find(cycle_id);
        if (it != m_cycles.end())
          fname = it->second.bk_filename;
      }

      bk.transfer_rate =
          is_from_trdisp ? get_transfer_rate_in() : get_transfer_rate_out();
      bk.datafilter_id = get_datafilter_id();

      // For FRW completion BK, inject filtered trigger numbers collected by
      // DataFilterReceiver so they appear in the JSON entry alongside the
      // written TRs, making filtering decisions explicit.
      const bool is_frw_completion =
          is_from_writer &&
          (bk.tr_status == to_string(TRStatus::kFileCompleted) ||
           bk.tr_status == to_string(TRStatus::kWriteFailed));
      // Computed before the enqueue so the finalize sentinel can be pushed in
      // the same lock, guaranteeing it lands in the same writer batch as the
      // kReRecorded entry and the file is retired immediately after its final
      // write.
      const bool is_trd_final =
          is_from_trdisp && (bk.tr_status == to_string(TRStatus::kReRecorded) ||
                             bk.tr_status == to_string(TRStatus::kWriteFailed));
      if (is_frw_completion && cycle_id != UINT64_MAX) {
        std::lock_guard<std::mutex> lk(m_cycles_mtx);
        auto it = m_cycles.find(cycle_id);
        if (it != m_cycles.end()) {
          for (uint64_t trig : it->second.filtered_triggers)
            bk.tr_header_info.push_back(
                {"filtered_trigger_number", std::to_string(trig)});
        }
      }

      {
        std::lock_guard<std::mutex> lk(queue_mutex);
        bk_queue.emplace(fname, to_json(bk));
        if (is_trd_final && !fname.empty())
          bk_queue.emplace(fname, nlohmann::json{}); // finalize sentinel
      }
      queue_cv.notify_one();

      if (is_frw_completion) {
        if (cycle_id != UINT64_MAX) {
          std::lock_guard<std::mutex> lk(m_cycles_mtx);
          auto it = m_cycles.find(cycle_id);
          if (it != m_cycles.end())
            it->second.frw_confirmed = true;
        }
        send_completion_to_trd(bk);
      }

      if (is_trd_final) {
        if (cycle_id != UINT64_MAX) {
          std::lock_guard<std::mutex> lk(m_cycles_mtx);
          m_cycles.erase(cycle_id);
        }
        const int prev =
            m_active_cycles.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 1) {
          // Last active cycle just completed.
          m_all_done_cv.notify_all();
        }
        TLOG() << "Cycle " << cycle_id << " complete (active=" << (prev - 1)
               << ")";
      }
    };

    cb_receiver->add_callback(str_receiver_cb);
    callback_registered.store(true, std::memory_order_release);
    TLOG() << "Callback registered, entering main loop";

    while (!stop_flag.load())
      std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cb_receiver->remove_callback();
    queue_cv.notify_all();
    TLOG() << "Receiver cleanup complete";
  }

  nlohmann::json to_json(const BookKeeping &bk) {
    return nlohmann::json{{"entry_id", bk.entry_id},
                          {"conn_id", bk.conn_id},
                          {"from_id", bk.from_id},
                          {"datafilter_id", bk.datafilter_id},
                          {"node", bk.node},
                          {"tr_header_info", bk.tr_header_info},
                          {"file_attributes_info", bk.file_attributes_info},
                          {"tr_status", bk.tr_status},
                          {"file_send_list", bk.file_send_list},
                          {"file_send_status", bk.file_send_status},
                          {"transfer_rate", bk.transfer_rate}};
  }

  void write_to_file() {
    TLOG() << "Setting up bookkeeping writer";
    // Per-file accumulator: filename -> array of JSON entries.
    std::unordered_map<std::string, nlohmann::json> file_data;

    for (;;) {
      std::unique_lock<std::mutex> lock(queue_mutex);
      if (!queue_cv.wait_for(lock, std::chrono::milliseconds(200), [&] {
            return !bk_queue.empty() || stop_flag.load();
          })) {
        if (stop_flag.load() && bk_queue.empty())
          break;
        continue;
      }
      if (bk_queue.empty() && stop_flag.load())
        break;

      std::vector<std::pair<std::string, nlohmann::json>> batch;
      batch.reserve(bk_queue.size());
      while (!bk_queue.empty()) {
        batch.emplace_back(std::move(bk_queue.front()));
        bk_queue.pop();
      }
      lock.unlock();

      // Append to per-file accumulators.
      // Null-JSON entries are finalize sentinels pushed by receive_bk() when
      // a cycle's kReRecorded arrives; they trigger retirement after one final
      // write.
      std::unordered_set<std::string> pending;
      std::unordered_set<std::string> finalized;

      for (auto &[fname, entry] : batch) {
        if (fname.empty()) {
          TLOG() << "write_to_file: BK entry has no filename, dropping";
          continue;
        }
        if (entry.is_null()) {
          finalized.insert(fname);
          pending.insert(fname); // flush one final time before retirement
          continue;
        }
        auto &arr = file_data[fname];
        if (!arr.is_array()) {
          arr = nlohmann::json::array();
          // Pre-load existing on-disk content so a re-dispatch after HD
          // failure + remount appends to the failure record rather than
          // overwriting it.
          std::ifstream existing(fname);
          if (existing.is_open()) {
            try {
              nlohmann::json disk_data;
              existing >> disk_data;
              if (disk_data.is_array())
                arr = std::move(disk_data);
            } catch (const std::exception &e) {
              TLOG() << "write_to_file: could not parse existing '" << fname
                     << "': " << e.what() << " -- starting fresh";
            }
          }
        }
        arr.push_back(std::move(entry));
        pending.insert(fname);
      }

      // Flush only files that received new entries in this batch.
      for (const auto &fname : pending) {
        auto it = file_data.find(fname);
        if (it == file_data.end())
          continue;
        auto &arr = it->second;
        // Maintain entries sorted by entry_id using a multimap.
        // nlohmann::json array iterators and std::sort interact
        // unpredictably, so we rebuild the array from a sorted container.
        std::multimap<std::string, nlohmann::json> sorted;
        for (auto &elem : arr)
          sorted.emplace(elem["entry_id"].get<std::string>(), std::move(elem));
        arr = nlohmann::json::array();
        for (auto &[_, elem] : sorted)
          arr.push_back(std::move(elem));

        std::ofstream f(fname);
        if (f.is_open())
          f << arr.dump(4);
        else
          TLOG() << "write_to_file: failed to open '" << fname << "'";
      }

      // Retire completed files so they are never rewritten by later batches.
      for (const auto &fname : finalized) {
        file_data.erase(fname);
        TLOG() << "write_to_file: retired " << fname << " (cycle complete)";
      }
    }
    TLOG() << "File writer thread exiting";
  }

  std::string generate_bk_filename(int run_number, int file_index) {
    std::ostringstream oss;
    oss << "bookkeeping_" << std::setw(6) << std::setfill('0') << run_number
        << "_" << std::setw(4) << std::setfill('0') << file_index << ".json";
    return oss.str();
  }
};

} // namespace datafilter
} // namespace dunedaq

#endif // DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_
