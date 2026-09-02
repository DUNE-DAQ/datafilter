/**
 * @file FilterResultWriter.cpp
 *
 * Implementations of FilterResultWriter's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "FilterResultWriter.hpp"

using dunedaq::datafilter::FilterResultWriter;

namespace {

bool has_enough_space(const std::string &dir, std::uintmax_t min_free_bytes) {
  std::error_code ec;
  auto sp = std::filesystem::space(dir, ec);
  if (ec) {
    TLOG() << "StorageCheck: cannot query space for " << dir << " ("
           << ec.message() << ") -- proceeding anyway";
    return true; // don't block on query failure
  }
  if (sp.available < min_free_bytes) {
    TLOG() << "StorageCheck: STORAGE LOW -- available=" << sp.available
           << " bytes (<" << min_free_bytes << "), skipping write to " << dir;
    return false;
  }
  return true;
}
} // anonymous namespace

namespace dunedaq::datafilter {

FilterResultWriter::FilterResultWriter(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_bk_thread(std::bind(&FilterResultWriter::receive_attrs, this,
                            std::placeholders::_1)) {
  register_command("conf", &FilterResultWriter::do_conf);
  register_command("start", &FilterResultWriter::do_start);
  register_command("stop", &FilterResultWriter::do_stop);
}

void FilterResultWriter::FilterResultWriter::init(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();
  m_mcfg = mcfg;

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  // get attributes (it is moving from TRD->DF->FRW).
  auto mdal =
      mcfg->get_dal<dunedaq::datafilter::dal::FilterResultWriter>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_cx = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);
  TLOG() << "FRW connections: "
         << "trwriter_ctrl="
         << (m_cx.trwriter_ctrl.empty() ? "<none>" : m_cx.trwriter_ctrl.front())
         << " tr_data_rx="
         << (m_cx.tr_data_rx.empty() ? "<none>" : m_cx.tr_data_rx.front())
         << " bk_in="
         << (m_cx.bk_inputs.empty() ? "<none>" : m_cx.bk_inputs.front())
         << " bk_out="
         << (m_cx.bk_outputs.empty() ? "<none>" : m_cx.bk_outputs.front());

  m_odir = mdal->get_odir();
  m_output_h5_filename = mdal->get_output_h5_filename();
  m_min_free_bytes = static_cast<std::uintmax_t>(mdal->get_min_free_bytes());
  TLOG() << "odir " << m_odir << " output_h5_filename prefix "
         << m_output_h5_filename << " min_free_bytes=" << m_min_free_bytes;
}

void FilterResultWriter::do_conf(const data_t &) {
  TLOG() << get_name() << " do_conf()";
  dunedaq::opmonlib::TestOpMonManager opmgr;
  try {
    TLOG() << "Configure IOManager...";
    get_iomanager()->configure(m_session_name, m_queues, m_networkconnections,
                               nullptr, opmgr);
  } catch (const std::exception &e) {
    TLOG() << "Failed to configure IOManager. " << e.what();
    throw;
  }

  // --kPubSub data channels--
  // Register callbacks immediately so ZMQ SUB sockets are subscribed before
  // any other app calls do_start() and publishes data.  Callbacks deposit
  // received data into per-type prebuf queues.
  if (!m_cx.ts_data_rx.empty() && !m_ts_prebuf_rx) {
    m_ts_prebuf_rx =
        dunedaq::get_iom_receiver<timeslice_ptr_t>(m_cx.ts_data_rx.front());
    m_ts_prebuf_rx->add_callback([this](timeslice_ptr_t &ts) {
      std::lock_guard<std::mutex> lk(m_ts_prebuf_mtx);
      m_ts_prebuf.push_back(std::move(ts));
      // notify_all: several cycle threads may be waiting here, each for its
      // own ts_number (see receive_ts_single_connection()) -- notify_one()
      // could repeatedly wake a thread whose item never arrives while the
      // one that should consume this push stays parked until its deadline.
      m_ts_prebuf_cv.notify_all();
    });
    TLOG() << "FRW: registered TS kPubSub callback on "
           << m_cx.ts_data_rx.front();
  }

  if (!m_cx.tr_data_rx.empty() && !m_tr_prebuf_rx) {
    m_tr_prebuf_rx = dunedaq::get_iom_receiver<trigger_record_ptr_t>(
        m_cx.tr_data_rx.front());
    m_tr_prebuf_rx->add_callback([this](trigger_record_ptr_t &tr) {
      std::lock_guard<std::mutex> lk(m_tr_prebuf_mtx);
      m_tr_prebuf.push_back(std::move(tr));
      // notify_all: see the identical reasoning on the TS callback above.
      m_tr_prebuf_cv.notify_all();
    });
    TLOG() << "FRW: registered TR kPubSub callback on "
           << m_cx.tr_data_rx.front();
  }

  // -- kSendRecv ctrl channels (trwriter0, tswriter0)
  // IOManager creates the ZMQ PULL socket lazily on the first
  // get_iom_receiver() call.  If receive_tr/ts_single_connection() is the first
  // caller (inside do_start()), the socket is created AFTER DF has already sent
  // "write_tr"/ "write_ts" on cold start — the message may be dropped before
  // the socket exists.  Calling get_iom_receiver() here pre-creates the sockets
  // so the TCP connection is established during do_conf(), long before
  // do_start().
  if (!m_cx.trwriter_ctrl.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.trwriter_ctrl.front());
    TLOG() << "FRW: pre-warmed trwriter_ctrl PULL on "
           << m_cx.trwriter_ctrl.front();
  }
  if (!m_cx.tswriter_ctrl.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.tswriter_ctrl.front());
    TLOG() << "FRW: pre-warmed tswriter_ctrl PULL on "
           << m_cx.tswriter_ctrl.front();
  }

  // Pre-warm BK input PULL socket so it exists before DF forwards BK1 on cold
  // start.
  if (!m_cx.bk_inputs.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
        m_cx.bk_inputs.front());
    TLOG() << "FRW: pre-warmed bk_inputs PULL on " << m_cx.bk_inputs.front();
  }

  TLOG() << get_name() << ": exist do_conf()";
}

void FilterResultWriter::do_start(const data_t &) {
  // Remove any zero-byte .filtered.writing files left by a previous crashed
  // run.
  std::error_code ec;
  for (auto &entry : std::filesystem::directory_iterator(m_odir, ec)) {
    const auto &p = entry.path();
    const auto &s = p.string();
    if (s.size() > 17 && s.substr(s.size() - 17) == ".filtered.writing" &&
        std::filesystem::file_size(p, ec) == 0) {
      TLOG() << "do_start: removing stale empty partial file: " << p;
      std::filesystem::remove(p, ec);
    }
  }

  m_bk_thread.start_working_thread();

  m_running.store(true);

  // Register always-on write_tr ctrl callback before the dispatch loop so no
  // handshake is dropped in the gap between cycle N's remove_callback() and
  // cycle N+1's add_callback() (same pattern as m_tr_prebuf_rx for kPubSub).
  if (!m_cx.trwriter_ctrl.empty()) {
    m_write_tr_ctrl_rx =
        dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
            m_cx.trwriter_ctrl.front());
    m_write_tr_ctrl_rx->add_callback(
        [this](dunedaq::datafilter::Handshake msg) {
          if (msg.msg_id == "write_tr") {
            std::lock_guard<std::mutex> lk(m_write_tr_prebuf_mtx);
            m_write_tr_prebuf.push(std::move(msg));
            m_write_tr_prebuf_cv.notify_one();
          }
        });
  }

  // Always-on write_ts ctrl callback (same pattern as write_tr).
  if (!m_cx.tswriter_ctrl.empty()) {
    m_write_ts_ctrl_rx =
        dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
            m_cx.tswriter_ctrl.front());
    m_write_ts_ctrl_rx->add_callback(
        [this](dunedaq::datafilter::Handshake msg) {
          if (msg.msg_id == "write_ts") {
            std::lock_guard<std::mutex> lk(m_write_ts_prebuf_mtx);
            m_write_ts_prebuf.push(std::move(msg));
            m_write_ts_prebuf_cv.notify_one();
          }
        });
  }

  // Each BK1 from DF produces one DispatchEntry.  Threads are spawned without
  // inline join so TR and TS threads for the same TRD cycle run concurrently
  // All active threads are joined when the dispatch loop exits.
  std::vector<std::thread> active_threads;
  while (m_running.load()) {
    DispatchEntry entry;
    {
      std::unique_lock<std::mutex> lk(m_dispatch_mtx);
      m_dispatch_cv.wait_for(lk, std::chrono::seconds(10), [this] {
        return !m_dispatch_queue.empty() || !m_running.load();
      });
      if (!m_running.load())
        break;
      if (m_dispatch_queue.empty())
        continue; // timeout: no entry yet, loop back and wait again
      entry = std::move(m_dispatch_queue.front());
      m_dispatch_queue.pop();
    }

    TLOG() << "FRW: dispatching record_type=" << entry.record_type
           << " cycle=" << entry.df_cycle_id;

    if (entry.record_type == "TS" && !m_cx.ts_data_rx.empty()) {
      active_threads.emplace_back(
          [this, cid = entry.df_cycle_id, seq = entry.trd_bk_seq,
           tot = entry.total_tr, fidx = entry.file_index,
           tsn = entry.ts_number] {
            receive_ts_single_connection(cid, seq, tot, fidx, tsn);
          });
    } else if (!m_cx.tr_data_rx.empty()) {
      // Default to TR path for "TR", empty string, or any unrecognised type.
      active_threads.emplace_back([this, cid = entry.df_cycle_id,
                                   seq = entry.trd_bk_seq,
                                   tot = entry.total_tr,
                                   fidx = entry.file_index,
                                   trn = entry.trigger_number] {
        receive_tr_single_connection(cid, seq, tot, fidx, trn);
      });
    }
  }
  TLOG() << "FRW: do_start() dispatch loop exiting, joining "
         << active_threads.size() << " threads";
  for (auto &t : active_threads)
    if (t.joinable())
      t.join();
}
void FilterResultWriter::do_stop(const data_t &) {
  m_running.store(false);
  m_dispatch_cv.notify_all();

  // Wake drain loops so they see m_running==false and exit.
  // Callbacks remain registered so ZMQ SUB stays subscribed for the next run.
  m_ts_prebuf_cv.notify_all();
  m_tr_prebuf_cv.notify_all();
  m_write_tr_prebuf_cv.notify_all();
  {
    std::lock_guard<std::mutex> lk(m_ts_prebuf_mtx);
    while (!m_ts_prebuf.empty())
      m_ts_prebuf.pop_front();
  }
  {
    std::lock_guard<std::mutex> lk(m_tr_prebuf_mtx);
    while (!m_tr_prebuf.empty())
      m_tr_prebuf.pop_front();
  }
  {
    std::lock_guard<std::mutex> lk(m_write_tr_prebuf_mtx);
    while (!m_write_tr_prebuf.empty())
      m_write_tr_prebuf.pop();
  }
  if (m_write_tr_ctrl_rx) {
    m_write_tr_ctrl_rx->remove_callback();
    m_write_tr_ctrl_rx.reset();
  }

  m_write_ts_prebuf_cv.notify_all();
  {
    std::lock_guard<std::mutex> lk(m_write_ts_prebuf_mtx);
    while (!m_write_ts_prebuf.empty())
      m_write_ts_prebuf.pop();
  }
  if (m_write_ts_ctrl_rx) {
    m_write_ts_ctrl_rx->remove_callback();
    m_write_ts_ctrl_rx.reset();
  }

  m_bk_thread.stop_working_thread();
}

std::string
FilterResultWriter::generate_hdf5file_pathname(std::string file_pathname_prefix,
                                               int run_number, int file_index,
                                               int trigger_number) {
  std::ostringstream filename_oss;
  filename_oss << file_pathname_prefix << "_" << std::setw(6)
               << std::setfill('0') << run_number << "_" << std::setw(4)
               << std::setfill('0') << file_index << "_" << trigger_number;
  return filename_oss.str();
}

dunedaq::hdf5libs::HDF5FileLayoutParameters create_file_layout_params() {
  dunedaq::hdf5libs::HDF5PathParameters params_tpc;
  params_tpc.detector_group_type = "Detector_Readout";
  params_tpc.detector_group_name = "TPC";
  params_tpc.element_name_prefix = "Link";
  params_tpc.digits_for_element_number = 5;

  std::vector<dunedaq::hdf5libs::HDF5PathParameters> param_list;
  param_list.push_back(params_tpc);

  dunedaq::hdf5libs::HDF5FileLayoutParameters layout_params;
  layout_params.path_params_list = param_list;
  layout_params.record_name_prefix = "TriggerRecord";
  layout_params.digits_for_record_number = 6;
  layout_params.digits_for_sequence_number = 0;
  layout_params.record_header_dataset_name = "TriggerRecordHeader";

  return layout_params;
}

void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
  TLOG() << "BookKeeping receive_attrs starting";

  if (m_cx.bk_inputs.empty()) {
    TLOG() << "FRW: no bookkeeping inputs configured; skipping receive_attrs";
    return;
  }
  const std::string &bk_rx_uid = m_cx.bk_inputs.front();

  auto receiver =
      dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(bk_rx_uid);
  if (!receiver) {
    TLOG() << "Failed to get BookKeeping receiver 'bookkeeping1'";
    return;
  }

  std::function<void(dunedaq::datafilter::BookKeeping &)> cb =
      [&](dunedaq::datafilter::BookKeeping bk) {
        if (bk.run_number > 0)
          m_run_number.store(bk.run_number);

        DispatchEntry entry;
        entry.run_number = static_cast<unsigned>(bk.run_number);

        for (const auto &kv : bk.file_attributes_info) {
          if (kv.first == "file_index") {
            try {
              entry.file_index = std::stoi(kv.second);
              FilterResultWriter::set_file_index(entry.file_index);
            } catch (const std::exception &e) {
              TLOG() << "Invalid file_index: '" << kv.second << "' ("
                     << e.what() << ")";
            }
          } else if (kv.first == "df_cycle_id") {
            try {
              entry.df_cycle_id = std::stoull(kv.second);
            } catch (...) {
            }
          } else if (kv.first == "trd_bk_seq") {
            try {
              entry.trd_bk_seq = std::stoull(kv.second);
            } catch (...) {
            }
          } else if (kv.first == "record_type") {
            entry.record_type = kv.second;
          } else if (kv.first == "total_tr") {
            try {
              entry.total_tr = std::stoi(kv.second);
            } catch (...) {
            }
          } else if (kv.first == "trigger_number") {
            try {
              entry.trigger_number = std::stoll(kv.second);
            } catch (...) {
            }
          } else if (kv.first == "ts_number") {
            try {
              entry.ts_number = std::stoll(kv.second);
            } catch (...) {
            }
          }
        }

        TLOG_DEBUG(1) << "BK1 from " << bk.from_id
                      << " run=" << entry.run_number
                      << " file_index=" << entry.file_index
                      << " record_type=" << entry.record_type
                      << " df_cycle_id=" << entry.df_cycle_id;

        {
          std::lock_guard<std::mutex> lk(m_dispatch_mtx);
          m_dispatch_queue.push(entry);
        }
        m_dispatch_cv.notify_one();
        TLOG() << "FRW: dispatch entry queued (record_type="
               << entry.record_type << " cycle=" << entry.df_cycle_id << ")";
      };

  TLOG() << "Registering BookKeeping callback";
  receiver->add_callback(cb);

  // Keep callback alive for the entire run
  while (running.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TLOG() << "Removing BookKeeping callback";
  receiver->remove_callback();

  TLOG() << "BookKeeping receive_attrs exiting";
}

void FilterResultWriter::receive_tr_single_connection(uint64_t df_cycle_id,
                                                       uint64_t trd_bk_seq,
                                                       int total_tr,
                                                       int file_index,
                                                       int64_t expected_trigger_number) {
  const int bk_total_tr = total_tr;  // save BK dispatch count (file record count)

  // Each write_tr ctrl carries total_tr=batch_size for kept TRs or 0 for
  // filtered ones. Wait up to 2s for DF to start, then drain all immediately
  // queued ctrls. Each write_tr(0) means one TR was filtered; decrement
  // total_tr so total_expected reflects what will actually arrive at FRW.
  // Late-arriving write_tr(0) ctrls are drained inside the TR receive loop.
  {
    std::unique_lock<std::mutex> lk(m_write_tr_prebuf_mtx);
    m_write_tr_prebuf_cv.wait_for(lk, std::chrono::seconds(2),
        [this] { return !m_write_tr_prebuf.empty(); });
    while (!m_write_tr_prebuf.empty()) {
      if (m_write_tr_prebuf.front().total_tr == 0) {
        --total_tr;
        TLOG() << "FRW: initial write_tr(0) ctrl drained, total_tr now " << total_tr;
      }
      m_write_tr_prebuf.pop();
    }
  }
  if (total_tr < 0) total_tr = 0;
  m_num_messages.store(total_tr);

  HDF5FileLayoutParameters fl_pars = create_file_layout_params();
  auto srcid_geoid_map = create_srcid_geoid_map();

  // Use a single connection for all TRs
  // std::string single_connection = "conn_A1_G0_C0_";
  // TLOG() << "Listening for ALL TRs on single connection: " <<
  // single_connection; TLOG() << "Expecting " << m_num_messages << " TRs
  // total";

  // Use a single connection for all TRs (discovered)
  if (m_cx.tr_data_rx.empty()) {
    TLOG() << "FRW: no TR data inputs configured; cannot receive TRs";
    return;
  }
  const std::string &single_connection = m_cx.tr_data_rx.front();
  TLOG() << "Listening for ALL TRs on single connection: " << single_connection;

std::atomic<size_t> total_msgs_received{0};
  // total_expected is the post-filter count: how many TRs DF forwarded to FRW.
  // bk_total_tr is the pre-filter dispatch count: used only in bookkeeping
  // output to report trs_dispatched_by_trd and trs_filtered_upstream.
  size_t total_expected = static_cast<size_t>(total_tr);
  TLOG() << "FRW: total_expected=" << total_expected
         << " (dispatched=" << bk_total_tr
         << ", initially_filtered=" << (bk_total_tr - total_tr) << ")";

  struct TRWriteRecord {
    std::string pathname;
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t file_index;
  };
  std::mutex pathnames_mutex;
  std::vector<TRWriteRecord> written_trs;
  size_t tr_write_errors = 0;

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  // Drain from the always-on prebuf (registered in do_start() before the
  // dispatch gate) so data received before this call is not lost.
  // Skip entirely when total_expected==0 (DataFilter filtered all TRs).
  TLOG() << "Waiting for " << total_expected << " TRs from prebuf...";
  const int kWatchdogSec = 60;
  auto tr_watchdog = std::chrono::steady_clock::now() +
                     std::chrono::seconds(kWatchdogSec);
  TLOG() << "FRW: idle watchdog=" << kWatchdogSec << "s (resets on each TR received)";
  while (m_running.load() && total_expected > 0) {
    // Drain any write_tr(0) ctrls that arrived since the last iteration;
    // each one represents a TR filtered by DataFilter that will never arrive.
    {
      std::lock_guard<std::mutex> ctrl_lk(m_write_tr_prebuf_mtx);
      while (!m_write_tr_prebuf.empty()) {
        if (m_write_tr_prebuf.front().total_tr == 0) {
          if (total_expected > 0) --total_expected;
          TLOG() << "FRW: write_tr(0) ctrl drained in loop, total_expected now "
                 << total_expected;
        }
        m_write_tr_prebuf.pop();
      }
    }
    if (total_msgs_received.load() >= total_expected)
      break;

    trigger_record_ptr_t tr;
    {
      // Several cycle threads share m_tr_prebuf (do_start() runs one thread
      // per dispatch entry, unjoined). Pick out the TR that is actually this
      // cycle's own rather than whichever one is at the front -- otherwise a
      // faster-arriving TR meant for a different cycle gets stolen and this
      // cycle's own TR is later stolen by someone else in turn. When
      // expected_trigger_number is unknown (storage mode never runs cycles
      // concurrently -- see get_from_storage()'s in-flight-file guard),
      // begin() acts as plain FIFO, matching the old behaviour exactly.
      auto find_mine = [expected_trigger_number](
                            std::deque<trigger_record_ptr_t> &q) {
        if (expected_trigger_number < 0)
          return q.begin();
        return std::find_if(
            q.begin(), q.end(), [&](const trigger_record_ptr_t &cand) {
              return !cand->get_fragments_ref().empty() &&
                     static_cast<int64_t>(cand->get_fragments_ref()
                                              .at(0)
                                              ->get_trigger_number()) ==
                         expected_trigger_number;
            });
      };

      std::unique_lock<std::mutex> lk(m_tr_prebuf_mtx);
      m_tr_prebuf_cv.wait_for(lk, std::chrono::milliseconds(200),
          [this, &find_mine] {
            return !m_running.load() ||
                   find_mine(m_tr_prebuf) != m_tr_prebuf.end();
          });
      if (!m_running.load())
        break;
      auto it = find_mine(m_tr_prebuf);
      if (it == m_tr_prebuf.end()) {
        if (std::chrono::steady_clock::now() >= tr_watchdog) {
          TLOG() << "FRW: idle watchdog fired (" << kWatchdogSec
                 << "s) after " << total_msgs_received.load()
                 << "/" << total_expected << " TRs";
          break;
        }
        continue;
      }
      tr = std::move(*it);
      m_tr_prebuf.erase(it);
      tr_watchdog = std::chrono::steady_clock::now() +
                    std::chrono::seconds(kWatchdogSec);
    }

    const size_t trigger_timestamp =
        tr->get_fragments_ref().at(0)->get_trigger_timestamp();
    const size_t trigger_number =
        tr->get_fragments_ref().at(0)->get_trigger_number();
    m_run_number.store(tr->get_fragments_ref().at(0)->get_run_number());

    size_t current_total = ++total_msgs_received;

    TLOG() << "Received TR " << current_total << "/" << total_expected
           << " - run: " << m_run_number.load()
           << ", trigger: " << trigger_number
           << ", file_index: " << file_index;

    // Write each TR to its own file
    std::string app_name = "test";
    std::string file_pathname_prefix = m_odir + "/" + m_output_h5_filename;
    std::string file_base =
        generate_hdf5file_pathname(file_pathname_prefix, m_run_number.load(),
                                   file_index, trigger_number);
    std::string writing_pathname = file_base + ".filtered.writing";
    std::string final_pathname = file_base + ".filtered.hdf5";

    TLOG() << "Writing TR " << current_total << "/" << total_expected << " to "
           << writing_pathname;

    if (!has_enough_space(m_odir, m_min_free_bytes)) {
      TLOG() << "Skipping TR write — insufficient storage in " << m_odir;
      return;
    }

    // Remove any stale .writing file from a previous failed attempt so that
    // HDF5RawDataFile always starts with a fresh file.
    if (std::filesystem::exists(writing_pathname)) {
      TLOG() << "FRW: removing stale writing file: " << writing_pathname;
      std::filesystem::remove(writing_pathname);
    }

    unsigned compression_level = 0;

    try {
      std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
          writing_pathname, m_run_number.load(), file_index, app_name,
          fl_pars, srcid_geoid_map, compression_level, ""));

      size_t tr_bytes = 0;
      for (const auto &frag : tr->get_fragments_ref())
        tr_bytes += frag->get_size();
      const auto t_wr_start = std::chrono::steady_clock::now();

      h5file_ptr->write(*tr);
      h5file_ptr.reset();

      const double t_wr_secs =
          std::chrono::duration_cast<std::chrono::duration<double>>(
              std::chrono::steady_clock::now() - t_wr_start).count();
      if (t_wr_secs > 0 && tr_bytes > 0) {
        const double wr_mbps =
            (static_cast<double>(tr_bytes) * 8.0) / t_wr_secs / 1e6;
        update_ewma(wr_mbps, m_tr_write_rate);
        TLOG() << "TR write rate: " << wr_mbps << " Mbps  EWMA: "
               << m_tr_write_rate.ewma_mbps.load(std::memory_order_relaxed)
               << " Mbps";
      }

      std::filesystem::rename(writing_pathname, final_pathname);
      TLOG() << "Successfully wrote TR " << current_total
             << " with trigger_number " << trigger_number << " -> "
             << final_pathname;

      {
        std::lock_guard<std::mutex> lock(pathnames_mutex);
        written_trs.push_back({final_pathname, trigger_number,
                               trigger_timestamp, file_index});
      }

    } catch (const std::exception &e) {
      TLOG() << "ERROR writing TR " << current_total << ": " << e.what();
      ++tr_write_errors;
    }

    // Check if all expected messages received
    if (current_total >= total_expected) {
      TLOG() << "All " << total_expected << " TRs received and written";
      break;
    }
  }

  if (total_msgs_received.load() < total_expected) {
    TLOG() << "WARNING: Timeout or stop before all TRs received. Got "
           << total_msgs_received.load() << "/" << total_expected;
  }

  {
    // Send final bookkeeping (always, even when total_expected==0 / all
    // filtered)
    TLOG() << "Send final bookkeeping info to datafilter server";
    TLOG() << "Total files written: " << written_trs.size();
    TLOG() << "Expected: " << total_expected
           << ", Actual: " << total_msgs_received.load();

    const size_t actually_received = total_msgs_received.load();
    const size_t actually_written = written_trs.size();

    if (actually_received < total_expected) {
      TLOG() << "FRW: received " << actually_received << "/" << total_expected
             << " TRs — " << (total_expected - actually_received)
             << " filtered upstream by DataFilter (expected)";
    }
    if (actually_written < actually_received) {
      TLOG() << "WARNING: wrote only " << actually_written << "/"
             << actually_received << " TRs received — write failures!";
    }

    dunedaq::datafilter::BookKeeping final_bk_info("bookkeeping0");
    final_bk_info.entry_id =
        time_point_to_string(std::chrono::system_clock::now());
    final_bk_info.conn_id = single_connection;
    final_bk_info.from_id = "FilterResultWriter";
    // kFileCompleted when all received TRs were either written or hit a
    // per-TR content error (e.g. HDF5 path collision in source data).
    // kWriteFailed is reserved for infrastructure failures where nothing
    // was written -- that keeps the 30-second retry backoff in TRD active.
    const size_t actually_processed = actually_written + tr_write_errors;
    if (tr_write_errors > 0)
      TLOG() << "FRW: " << tr_write_errors
             << " TR(s) failed to write (content errors, not retried).";
    // actually_received==0 is only a legitimate success when nothing was
    // ever expected (DF filtered every TR, total_expected==0 from the
    // start). If total_expected>0 but nothing was received, this thread's
    // specific expected_trigger_number never showed up in m_tr_prebuf
    // before the watchdog fired -- an orphaned record, not a success.
    const bool orphaned = (actually_received == 0 && total_expected > 0);
    final_bk_info.tr_status =
        (!orphaned && actually_processed >= actually_received)
            ? to_string(TRStatus::kFileCompleted)
            : to_string(TRStatus::kWriteFailed);
    final_bk_info.run_number = m_run_number.load();
    final_bk_info.tr_header_info.push_back(
        {"run_number", std::to_string(m_run_number.load())});
    final_bk_info.tr_header_info.push_back(
        {"total_trs_written", std::to_string(actually_written)});
    final_bk_info.tr_header_info.push_back(
        {"trs_received_from_df", std::to_string(actually_received)});
    final_bk_info.tr_header_info.push_back(
        {"trs_dispatched_by_trd", std::to_string(bk_total_tr)});
    final_bk_info.tr_header_info.push_back(
        {"trs_filtered_upstream",
         std::to_string(static_cast<size_t>(bk_total_tr) > actually_received
                            ? static_cast<size_t>(bk_total_tr) - actually_received
                            : 0)});
    if (df_cycle_id != UINT64_MAX)
      final_bk_info.file_attributes_info.push_back(
          {"df_cycle_id", std::to_string(df_cycle_id)});
    if (trd_bk_seq != UINT64_MAX)
      final_bk_info.file_attributes_info.push_back(
          {"trd_bk_seq", std::to_string(trd_bk_seq)});

    for (size_t i = 0; i < actually_written; ++i) {
      final_bk_info.tr_header_info.push_back({"file", written_trs[i].pathname});
      final_bk_info.tr_header_info.push_back(
          {"trigger_number", std::to_string(written_trs[i].trigger_number)});
      final_bk_info.tr_header_info.push_back(
          {"trigger_timestamp",
           std::to_string(written_trs[i].trigger_timestamp)});
    }

    auto final_bookkeeping_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
            "bookkeeping0");
    final_bookkeeping_sender->send(std::move(final_bk_info), Sender::s_block);
  }

  m_num_messages = 0;
  TLOG_DEBUG(5) << "receive_tr_single_connection() done";
}

void FilterResultWriter::receive_ts_single_connection(uint64_t df_cycle_id,
                                                      uint64_t trd_bk_seq,
                                                      int total_ts,
                                                      int file_index,
                                                      int64_t expected_ts_number) {
  if (m_cx.ts_data_rx.empty()) {
    TLOG_DEBUG(7) << "FRW: no TS data inputs configured; skipping TS receive";
    return;
  }

  // Seeded from the dispatch entry, as the TR path does. The write_ts ctrl
  // below only refines it: several TS threads share one ctrl queue, so a
  // thread that loses that race must not fall back to expecting zero.
  std::atomic<size_t> ts_expected{
      static_cast<size_t>(total_ts < 0 ? 0 : total_ts)};

  // Use always-on prebuf for write_ts ctrl (registered in do_start()).
  if (!m_cx.tswriter_ctrl.empty()) {
    std::unique_lock<std::mutex> lk(m_write_ts_prebuf_mtx);
    bool got_ctrl =
        m_write_ts_prebuf_cv.wait_for(lk, std::chrono::seconds(2), [this] {
          return !m_write_ts_prebuf.empty();
        });
    if (got_ctrl && !m_write_ts_prebuf.empty()) {
      ts_expected.store(m_write_ts_prebuf.front().total_tr);
      TLOG() << "FRW: write_ts ctrl received, total=" << ts_expected.load();
      m_write_ts_prebuf.pop();
    } else {
      TLOG_DEBUG(7) << "FRW: no write_ts ctrl within 2s";
    }
  }

  dunedaq::hdf5libs::HDF5FileLayoutParameters ts_fl_pars;
  dunedaq::hdf5libs::HDF5PathParameters params_tpc;
  params_tpc.detector_group_type = "Detector_Readout";
  params_tpc.detector_group_name = "TPC";
  params_tpc.element_name_prefix = "Link";
  params_tpc.digits_for_element_number = 5;
  ts_fl_pars.path_params_list.push_back(params_tpc);
  ts_fl_pars.record_name_prefix = "TimeSlice";
  ts_fl_pars.digits_for_record_number = 6;
  ts_fl_pars.digits_for_sequence_number = 0;
  ts_fl_pars.record_header_dataset_name = "TimeSliceHeader";

  auto srcid_geoid_map = create_srcid_geoid_map();

  TLOG() << "Listening for TimeSlices, expecting " << ts_expected.load();

  std::atomic<size_t> ts_received{0};
  std::atomic<size_t> ts_written{0};
  size_t ts_write_errors = 0;
  struct TSWriteRecord {
    std::string pathname;
    size_t ts_number;
  };
  std::vector<TSWriteRecord> written_ts_records;

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(5 + ts_expected.load());
  while (m_running.load()) {
    timeslice_ptr_t ts;
    {
      // Same reasoning as the TR consumer above: several cycle threads share
      // m_ts_prebuf, so pick out this cycle's own TimeSlice by ts_number
      // rather than taking whichever one is at the front. Unknown
      // expected_ts_number (storage mode) falls back to plain FIFO via
      // begin().
      auto find_mine = [expected_ts_number](std::deque<timeslice_ptr_t> &q) {
        if (expected_ts_number < 0)
          return q.begin();
        return std::find_if(
            q.begin(), q.end(), [&](const timeslice_ptr_t &cand) {
              return static_cast<int64_t>(
                         cand->get_header().timeslice_number) ==
                     expected_ts_number;
            });
      };

      std::unique_lock<std::mutex> lk(m_ts_prebuf_mtx);
      m_ts_prebuf_cv.wait_until(lk, deadline, [this, &find_mine] {
        return !m_running.load() ||
               find_mine(m_ts_prebuf) != m_ts_prebuf.end();
      });
      if (!m_running.load())
        break;
      auto it = find_mine(m_ts_prebuf);
      if (it == m_ts_prebuf.end())
        break;
      ts = std::move(*it);
      m_ts_prebuf.erase(it);
    }

    ++ts_received;
    auto ts_number = ts->get_header().timeslice_number;
    size_t current = ts_received.load();

    TLOG() << "Received TS " << current << "/" << ts_expected.load()
           << " ts_number=" << ts_number;

    std::string file_pathname_prefix =
        m_odir + "/" + m_output_h5_filename + "_ts";
    std::string file_base = generate_hdf5file_pathname(
        file_pathname_prefix, m_run_number.load(), file_index, ts_number);
    std::string writing_pathname = file_base + ".filtered.writing";
    std::string final_pathname = file_base + ".filtered.hdf5";

    if (!has_enough_space(m_odir, m_min_free_bytes)) {
      TLOG() << "Skipping TS write — insufficient storage in " << m_odir;
    } else {
      unsigned compression_level = 0;
      try {
        std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
            writing_pathname, m_run_number.load(), file_index, "test",
            ts_fl_pars, srcid_geoid_map, compression_level, ""));
        size_t ts_bytes = 0;
        for (const auto &frag : ts->get_fragments_ref())
          ts_bytes += frag->get_size();
        const auto t_ts_start = std::chrono::steady_clock::now();

        h5file_ptr->write(*ts);
        h5file_ptr.reset();

        const double t_ts_secs =
            std::chrono::duration_cast<std::chrono::duration<double>>(
                std::chrono::steady_clock::now() - t_ts_start).count();
        if (t_ts_secs > 0 && ts_bytes > 0) {
          const double wr_mbps =
              (static_cast<double>(ts_bytes) * 8.0) / t_ts_secs / 1e6;
          update_ewma(wr_mbps, m_ts_write_rate);
          TLOG() << "TS write rate: " << wr_mbps << " Mbps  EWMA: "
                 << m_ts_write_rate.ewma_mbps.load(std::memory_order_relaxed)
                 << " Mbps";
        }

        std::filesystem::rename(writing_pathname, final_pathname);
        ++ts_written;
        written_ts_records.push_back({final_pathname, ts_number});
        TLOG() << "Successfully wrote TS " << current
               << " ts_number=" << ts_number << " -> " << final_pathname;
      } catch (const std::exception &e) {
        ++ts_write_errors;
        TLOG() << "ERROR writing TS: " << e.what();
      }
    }

    if (ts_expected.load() > 0 && current >= ts_expected.load()) {
      // Reaching the count is not enough to leave: these threads are one-shot
      // (one per dispatch cycle) and do_stop() discards whatever is still
      // queued, so a TS left behind here is lost for good. Keep draining while
      // a backlog exists. Deliberately NOT bounded by the deadline: DF paces
      // sends 100 ms apart over a window longer than any single thread's
      // nominal lifetime, so capping the drain here strands every TS that
      // arrives after the last cycle's deadline. The loop still exits on
      // !m_running, and an empty queue re-arms the deadline in wait_until().
      bool backlog;
      {
        std::lock_guard<std::mutex> lk(m_ts_prebuf_mtx);
        backlog = !m_ts_prebuf.empty();
      }
      if (!backlog) {
        TLOG() << "All " << ts_expected.load() << " TSs received and written";
        break;
      }
    }
  }

  // Notify DF of TS batch completion
  if (!m_cx.bk_outputs.empty()) {
    // Mirror the TR path (see the kFileCompleted decision above). Now that
    // find_mine() matches this thread's own expected_ts_number, ts_received
    // reflects whether THIS cycle's TimeSlice actually showed up -- so
    // receiving none while one was expected (ts_expected>0) means the
    // record was orphaned (never arrived in m_ts_prebuf before wait_until's
    // deadline), not a vacuous success.
    const bool orphaned = (ts_expected.load() > 0 && ts_received.load() == 0);
    const bool all_written = !orphaned && (ts_write_errors == 0) &&
                             (ts_written.load() == ts_received.load());
    const auto ts_status = all_written ? to_string(TRStatus::kFileCompleted)
                                       : to_string(TRStatus::kWriteFailed);
    dunedaq::datafilter::BookKeeping ts_bk(m_cx.bk_outputs.front());
    ts_bk.entry_id = time_point_to_string(std::chrono::system_clock::now());
    ts_bk.from_id = "FilterResultWriter";
    ts_bk.run_number = m_run_number.load();
    ts_bk.tr_status = ts_status;
    if (df_cycle_id != UINT64_MAX)
      ts_bk.file_attributes_info.push_back(
          {"df_cycle_id", std::to_string(df_cycle_id)});
    if (trd_bk_seq != UINT64_MAX)
      ts_bk.file_attributes_info.push_back(
          {"trd_bk_seq", std::to_string(trd_bk_seq)});
    ts_bk.tr_header_info.push_back(
        {"total_ts_written", std::to_string(ts_written.load())});
    ts_bk.tr_header_info.push_back(
        {"expected_ts", std::to_string(ts_expected.load())});
    // int fi = get_file_index();
    for (size_t i = 0; i < written_ts_records.size(); ++i) {
      ts_bk.tr_header_info.push_back({"ts_file", written_ts_records[i].pathname});
      ts_bk.tr_header_info.push_back(
          {"ts_number", std::to_string(written_ts_records[i].ts_number)});
      // ++fi;
    }
    try {
      auto bk_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              m_cx.bk_outputs.front());
      bk_sender->send(std::move(ts_bk), Sender::s_block);
      TLOG() << "FRW: sent TS completion BK (" << ts_status << ") to DF"
             << " ts_received=" << ts_received.load()
             << " ts_expected=" << ts_expected.load();
    } catch (const std::exception &e) {
      TLOG() << "FRW: TS completion BK send failed: " << e.what();
    }
  }

  TLOG_DEBUG(5) << "receive_ts_single_connection() done";
}

// DF should alway send next_tr, so it is not used here. It will be removed in
// next cleanup.
void FilterResultWriter::send_next_tr() {
  auto sender_next_tr =
      dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("trdispatcher1");

  // std::chrono::milliseconds timeout(100);
  dunedaq::datafilter::Handshake sent_t1("next_tr");
  // sender_next_tr->send(std::move(sent_t1), timeout);
  sender_next_tr->send(std::move(sent_t1), Sender::s_block);
}

void FilterResultWriter::generate_opmon_data() {
  dunedaq::datafilter::opmon::FilterResultWriterInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

void FilterResultWriter::receive_attrs_test() {
  using BK = dunedaq::datafilter::BookKeeping;
  TLOG() << "receive_attrs_test: starting std::thread receiver";

  auto receiver = dunedaq::get_iom_receiver<BK>("bookkeeping1");
  if (!receiver) {
    TLOG() << "receive_attrs_test: failed to get 'bookkeeping1' receiver";
    m_attrs_test_running.store(false, std::memory_order_release);
    return;
  }

  std::function<void(BK &)> cb = [&](BK bk) {
    // Extract "file_index"
    for (const auto &kv : bk.file_attributes_info) {
      if (kv.first == "file_index") {
        try {
          const int idx = std::stoi(kv.second);
          // Ensure these are thread-safe (atomic or mutex-protected)
          FilterResultWriter::set_file_index(idx);
        } catch (const std::exception &e) {
          TLOG() << "receive_attrs_test: invalid file_index '" << kv.second
                 << "' (" << e.what() << ")";
        }
        break;
      }
    }

    TLOG_DEBUG(1) << "BookKeeping from " << bk.from_id
                  << " run=" << bk.run_number
                  << " file_index=" << FilterResultWriter::get_file_index();
  };

  TLOG() << "receive_attrs_test: registering callback";
  receiver->add_callback(cb);

  // Keep callback alive for entire run; block here until stop requested
  {
    std::unique_lock<std::mutex> lk(m_attrs_test_mtx);
    m_attrs_test_cv.wait(lk, [this] {
      return !m_attrs_test_running.load(std::memory_order_relaxed);
    });
  }

  TLOG() << "receive_attrs_test: removing callback and exiting";
  receiver->remove_callback();
}

// Using thread instead. It is not used
void FilterResultWriter::start_receive_attrs_test_thread() {
  // prevent double-start
  bool was_running =
      m_attrs_test_running.exchange(true, std::memory_order_acq_rel);
  if (was_running)
    return;

  m_attrs_test_thread =
      std::thread(&FilterResultWriter::receive_attrs_test, this);
}

void FilterResultWriter::stop_receive_attrs_test_thread() {
  bool was_running =
      m_attrs_test_running.exchange(false, std::memory_order_acq_rel);
  if (!was_running)
    return;

  // Wake the thread if it's waiting
  m_attrs_test_cv.notify_all();

  if (m_attrs_test_thread.joinable())
    m_attrs_test_thread.join();
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterResultWriter)
