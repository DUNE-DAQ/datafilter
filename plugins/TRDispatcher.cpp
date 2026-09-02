/**
 * @file TRDispatcher.cpp
 *
 * Implementations of TRDispatcher's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TRDispatcher.hpp"

#include <iomanip>

namespace dunedaq::datafilter {

TRDispatcher::TRDispatcher(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(std::bind(&TRDispatcher::do_work, this, std::placeholders::_1)) {
  register_command("conf", &TRDispatcher::do_conf);
  register_command("start", &TRDispatcher::do_start);
  register_command("stop", &TRDispatcher::do_stop);
}

void TRDispatcher::init(std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();

  m_mcfg = mcfg;

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  // get TRDispatcher attributes.
  auto mdal = mcfg->get_dal<dunedaq::datafilter::dal::TRDispatcher>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_cx = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);
  m_tr_tracking_tx = m_cx.tr_tracking_tx; // signal to DF
  m_tr_connections_o = m_cx.tr_data_tx;   // TriggerRecord outputs

  if (!m_cx.bk_outputs.empty())
    m_bk_connection_o = m_cx.bk_outputs.front(); // Bookkeeping out

  m_storage_pathname = mdal->get_storage_pathname();
  m_is_from_storage = mdal->get_is_from_storage();

  // Startup log only: the dispatch loop overwrites this per file.
  m_input_h5_filename = mdal->get_input_h5_filename();
  if (!m_is_from_storage)
    m_input_h5_filename = m_storage_pathname + "/" + m_input_h5_filename;

  m_json_file = mdal->get_json_file();
  m_generate_trigger_record = mdal->get_generate_trigger_record();
  if (m_generate_trigger_record)
    TLOG() << "Runing generated trigger record.";

  m_generate_time_slice = mdal->get_generate_time_slice();
  if (m_generate_time_slice)
    TLOG() << "Runing Generated TimeSlice";

  m_parallel_send = mdal->get_parallel_send();
  if (m_parallel_send)
    TLOG() << "Parallel send enabled.";

  m_generated_window = mdal->get_generated_window();
  TLOG() << "Generated-mode in-flight window: " << m_generated_window;

  // test events with limited number from oks
  m_number_generated_events = mdal->get_number_generated_events();
  if (m_number_generated_events > 0)
    TLOG() << "Max generated events: " << m_number_generated_events;

  m_send_timeout_ms = std::chrono::milliseconds(mdal->get_send_timeout_ms());
  m_recv_timeout_ms = std::chrono::milliseconds(mdal->get_recv_timeout_ms());

  TLOG() << "The storage for the HDF5 files is set to " << m_storage_pathname
         << " input_h5_filename " << m_input_h5_filename
         << "  m_is_from_storage " << m_is_from_storage;
}

void TRDispatcher::do_conf(const data_t &) {
  // auto iom = iomanager::IOManager::get();
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

  // use the first only, for now.
  m_trdispatcher_req_rx = m_cx.trdispatcher_req_rx.front();
  if (m_trdispatcher_req_rx.empty()) {
    TLOG() << "WARNING: No handshake receiver UID could be resolved from "
           << "ConnectionsBuilder::trdispatcher_req; falling back to legacy "
              "'trdispatcher0'.";
    m_trdispatcher_req_rx = "trdispatcher0"; // legacy fallback
  }

  // log discovered outputs
  for (auto &tr_tx : m_tr_connections_o) {
    TLOG() << "TR data TX discovered: " << tr_tx;
  }

  if (!m_bk_connection_o.empty()) {
    TLOG() << "Bookkeeping TX discovered: " << m_bk_connection_o;
  }

  // Pre-warm PULL sockets so they exist before DF/FRW send control/BK messages.
  // IOManager creates sockets lazily; without this, cold-start sends are
  // dropped.
  if (!m_cx.trdispatcher_req_rx.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.trdispatcher_req_rx.front());
    TLOG() << "TRD: pre-warmed trdispatcher_req_rx PULL on "
           << m_cx.trdispatcher_req_rx.front();
  }
  if (!m_cx.bk_inputs.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
        m_cx.bk_inputs.front());
    TLOG() << "TRD: pre-warmed bk_inputs PULL on " << m_cx.bk_inputs.front();
  }

  // Pre-create PUB/PUSH sender sockets in do_conf() so ZMQ connections
  // are established before any data flows in do_start().
  // This mitigates the ZMQ slow-joiner issue on cold start.
  if (!m_cx.tr_data_tx.empty()) {
    m_trdispatcher_id = m_cx.tr_data_tx.front();
    dunedaq::get_iom_sender<trigger_record_ptr_t>(m_trdispatcher_id);
    TLOG() << "TRD: pre-created TR data sender on " << m_trdispatcher_id;
  }

  TLOG() << get_name() << ": exist do_conf()";
}

void TRDispatcher::do_start(const data_t &) {
  TLOG() << "TRD do_start(): ENTER, m_start_barrier=" << m_start_barrier.load()
         << " m_keep_running=" << m_keep_running.load();

  // Generated mode reuses a fixed run_number (see the class member's default)
  // across separate test sessions. Without this, DF's bookkeeping writer
  // would merge this session's entries with leftover bookkeeping_<run>_*.json
  // files from an earlier session at the same run_number -- write_to_file()'s
  // reload-existing-content logic is meant for same-session retry after an
  // HD failure, not for a brand new session starting cold. Scoped to
  // generated mode only: storage mode's run_number comes from each source
  // HDF5 file's own attribute and legitimately varies, so there is no single
  // "this session's run_number" to scope a cleanup to.
  if (m_generate_trigger_record || m_generate_time_slice) {
    std::ostringstream prefix_oss;
    prefix_oss << "bookkeeping_" << std::setw(6) << std::setfill('0')
               << run_number << "_";
    const std::string prefix = prefix_oss.str();
    size_t removed = 0;
    std::error_code ec;
    for (const auto &dirent :
         std::filesystem::directory_iterator(".", ec)) {
      if (ec)
        break;
      const std::string fname = dirent.path().filename().string();
      if (fname.rfind(prefix, 0) == 0 &&
          fname.size() >= 5 &&
          fname.compare(fname.size() - 5, 5, ".json") == 0) {
        std::filesystem::remove(dirent.path(), ec);
        if (!ec)
          ++removed;
      }
    }
    if (removed > 0)
      TLOG() << "TRD do_start(): removed " << removed
             << " stale " << prefix << "*.json file(s) from a previous session";
  }

  m_keep_running.store(true);
  m_events_remaining.store(m_number_generated_events);
  m_events_remaining.store(m_number_generated_events);
  m_pub_warmup_needed.store(true);
  m_tr_pub_warmup_needed.store(true);
  m_ts_pub_warmup_needed.store(true);
  // Reset the generated-mode in-flight window: do_stop()'s bulk
  // m_bk_waiters.clear() does not run the per-waiter decrement, so a run
  // stopped mid-cycle would otherwise leave a stale non-zero count that
  // wedges the very next run's dispatch.
  m_tr_in_flight.store(0, std::memory_order_relaxed);
  m_ts_in_flight.store(0, std::memory_order_relaxed);

  // Always-on callback on bk_inputs (bookkeeping2): routes each confirmation
  // from DF to the correct in-flight CycleWaiter by trd_bk_seq.  Registered
  // before get_from_storage() so no confirmation can arrive before the
  // callback is active.
  if (!m_cx.bk_inputs.empty()) {
    m_bk_always_on_rx =
        dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
            m_cx.bk_inputs.front());
    m_bk_always_on_rx->add_callback([this](
                                        dunedaq::datafilter::BookKeeping bk) {
      if (bk.from_id != "FilterResultWriter")
        return;
      uint64_t seq = UINT64_MAX;
      for (const auto &kv : bk.file_attributes_info)
        if (kv.first == "trd_bk_seq") {
          try {
            seq = std::stoull(kv.second);
          } catch (...) {
          }
          break;
        }
      if (seq == UINT64_MAX) {
        TLOG() << "TRD always-on BK cb: missing trd_bk_seq, ignoring";
        return;
      }
      std::shared_ptr<CycleWaiter> waiter;
      {
        std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
        auto it = m_bk_waiters.find(seq);
        if (it != m_bk_waiters.end())
          waiter = it->second;
      }
      if (!waiter) {
        TLOG() << "TRD always-on BK cb: no waiter for seq=" << seq;
        return;
      }
      waiter->file_attrs = bk.file_attributes_info;
      waiter->got_reply.store(true, std::memory_order_release);

      // Send final BK (kReRecorded) directly from the callback so
      // send_tr/send_ts can return immediately without waiting.
      if (!m_bk_connection_o.empty()) {
        dunedaq::datafilter::time_point_to_string tp2s(
            dunedaq::datafilter::Precision::NANOSECONDS);
        dunedaq::datafilter::BookKeeping final_bk(m_bk_connection_o);
        // Use FRW's completion timestamp when available (propagated by DF relay);
        // fall back to now() only if relay did not set it.
        final_bk.entry_id = bk.entry_id.empty()
                                ? tp2s(std::chrono::system_clock::now())
                                : bk.entry_id;
        final_bk.from_id =
            waiter->is_hdf5_mode ? "trdispatcher" : "trdispatcher";
        final_bk.tr_status = to_string(TRStatus::kReRecorded);
        final_bk.run_number = bk.run_number;
        final_bk.tr_header_info.push_back(
            {"run number", std::to_string(bk.run_number)});
        for (const auto &kv : waiter->file_attrs)
          if (kv.first == "df_cycle_id") {
            final_bk.file_attributes_info.push_back(kv);
            break;
          }
        if (!waiter->file_send_list.empty())
          final_bk.file_send_list = waiter->file_send_list;
        try {
          auto bk_sender =
              dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
                  m_bk_connection_o);
          bk_sender->send(std::move(final_bk), std::chrono::milliseconds(2000));
          TLOG() << "TRD always-on BK cb: sent final BK (kReRecorded) seq="
                 << seq;
        } catch (const std::exception &e) {
          TLOG() << "TRD always-on BK cb: final BK send failed: " << e.what();
        }
      }

      // Deferred WriteJSON for HDF5 mode.
      // WriteJSON is called BEFORE releasing the in-flight guard to
      // eliminate the TOCTOU window where the file is neither in-flight
      // nor in the JSON (which lets the 100ms polling loop re-dispatch it).
      if (waiter->is_hdf5_mode && waiter->got_reply.load()) {
        if (bk.tr_status == to_string(TRStatus::kFileCompleted) ||
            bk.tr_status == to_string(TRStatus::kReRecorded)) {
          try {
            dunedaq::datafilter::HDF5FromStorage s(waiter->storage_pathname,
                                                   waiter->json_file);
            s.WriteJSON(waiter->h5_filename);
            TLOG() << "TRD always-on BK cb: WriteJSON done for "
                   << waiter->h5_filename;
          } catch (const std::exception &e) {
            TLOG() << "TRD always-on BK cb: WriteJSON failed: " << e.what();
          }
        } else {
          TLOG() << "TRD always-on BK cb: WriteJSON skipped -- FRW status="
                 << bk.tr_status << " for " << waiter->h5_filename;
          // Hold off re-dispatch for 30 s so a persistent write failure
          // does not spin the dispatch loop at full speed.
          {
            std::lock_guard<std::mutex> blk(m_backoff_mtx);
            m_backoff_files[waiter->h5_filename] =
                std::chrono::steady_clock::now() + std::chrono::seconds(30);
          }
        }
        {
          std::lock_guard<std::mutex> lk(m_in_flight_mtx);
          m_in_flight_files.erase(waiter->h5_filename);
        }
        m_in_flight_cv.notify_one();
      }

      // Clean up waiter
      {
        std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
        m_bk_waiters.erase(seq);
      }
      if (!waiter->is_hdf5_mode) {
        // Generated-mode cycle confirmed (success or failure alike): release
        // its in-flight window slot. TR and TS are gated independently.
        std::atomic<int> &counter =
            waiter->is_ts_waiter ? m_ts_in_flight : m_tr_in_flight;
        {
          std::lock_guard<std::mutex> wlk(m_gen_window_mtx);
          counter.fetch_sub(1, std::memory_order_acq_rel);
        }
        m_gen_window_cv.notify_all();
      }
      TLOG() << "TRD always-on BK cb: completed seq=" << seq;
    });
    TLOG() << "TRD: registered always-on BK callback on "
           << m_cx.bk_inputs.front();
  }

  // Always-on callback on trdispatcher_req_rx: buffers "next_tr"/"next_ts"
  // requests so they are never dropped between cycles.
  if (!m_cx.trdispatcher_req_rx.empty()) {
    m_req_rx = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.trdispatcher_req_rx.front());
    m_req_rx->add_callback([this](dunedaq::datafilter::Handshake msg) {
      if (msg.msg_id == "next_tr" || msg.msg_id == "next_ts") {
        TLOG() << "received request_next_tr from FO";
        std::lock_guard<std::mutex> lk(m_req_prebuf_mtx);
        m_req_prebuf.push(std::move(msg));
        m_req_prebuf_cv.notify_one();
      }
    });
    TLOG() << "TRD: registered always-on request callback on "
           << m_cx.trdispatcher_req_rx.front();
  }

  // Start the WorkerThread so get_from_storage() runs off the command thread.
  // If we call get_from_storage() directly here, the framework's sequential
  // command dispatch blocks: DF/FRW never get do_start() and never send
  // "next_tr", so we deadlock.
  TLOG() << "TRD do_start: starting WorkerThread";
  m_thread.start_working_thread();

  // Two-phase barrier: wait for WorkerThread to be ready, then set barrier.
  // In multi-module mode (do_start async), this ensures the barrier is set
  // before do_work() waits.  In standalone mode, the fallback timeout in
  // do_work() handles the case where do_start() blocks on
  // start_working_thread().
  TLOG() << "TRD do_start: waiting for m_worker_ready...";
  {
    std::unique_lock<std::mutex> lk(m_req_prebuf_mtx);
    bool worker_ready =
        m_req_prebuf_cv.wait_for(lk, std::chrono::milliseconds(100),
                                 [this] { return m_worker_ready.load(); });
    if (worker_ready) {
      TLOG() << "TRD do_start: m_worker_ready detected, setting barrier";
    } else {
      TLOG() << "TRD do_start: m_worker_ready TIMEOUT (standalone mode)";
    }
  }

  // WorkerThread is ready OR we timed out.  Set the barrier and notify.
  TLOG() << "TRD do_start: setting m_start_barrier=true";
  m_start_barrier.store(true);
  m_req_prebuf_cv.notify_all();
  TLOG() << "TRD do_start: EXIT, m_start_barrier=" << m_start_barrier.load();

  // NOTE: BK cleanup moved to do_stop(); the BK callback must stay alive
  // for the entire run because FRW sends completion BK asynchronously.
}

void TRDispatcher::get_from_storage() {
  // std::vector<std::filesystem::path> files;
  // size_t cnt = 0;

  TLOG() << "m_is_from_storage=" << m_is_from_storage
         << " m_generate_triger_record=" << m_generate_trigger_record
         << " m_generate_time_slice=" << m_generate_time_slice;

  const DispatchMode mode = [&]() -> DispatchMode {
    // is_from_storage is authoritative: when set, always dispatch real HDF5
    // files from storage_pathname and ignore the generate_* flags. Warn
    // loudly rather than overriding silently -- a silent override is what
    // made this combination confusing to configure in the first place.
    if (m_is_from_storage) {
      if (m_generate_trigger_record || m_generate_time_slice)
        TLOG() << "TRD: is_from_storage=1 overrides generate_trigger_record="
               << m_generate_trigger_record
               << " generate_time_slice=" << m_generate_time_slice
               << " -- dispatching from storage, not generating";
      return DispatchMode::kStorageHDF5;
    }
    if (!m_generate_trigger_record && !m_generate_time_slice)
      return DispatchMode::kStorageHDF5;
    if (m_generate_trigger_record && m_generate_time_slice && m_parallel_send)
      return DispatchMode::kGeneratedParallel;
    return DispatchMode::kGeneratedSerial;
  }();

  TLOG() << "get_from_storage: mode=" << static_cast<int>(mode)
         << " m_running_flag="
         << (m_running_flag ? m_running_flag->load() : -1);

  // kGeneratedSerial / kGeneratedParallel: no filesystem polling
  if (mode != DispatchMode::kStorageHDF5) {
    TLOG() << "get_from_storage: entering generated while loop";
    // Loop on both keep_running and running_flag:
    // - keep_running=false: do_stop() called and returned, exit immediately
    // - running_flag=false: stop_working_thread() called, framework wants us to
    // stop The CV wait inside receive() unblocks when either condition changes.
    while (m_keep_running.load() && m_running_flag && m_running_flag->load())
      receive(mode);
    TLOG() << "get_from_storage: exited generated while loop (keep_running="
           << m_keep_running.load()
           << " running_flag=" << (m_running_flag ? m_running_flag->load() : -1)
           << ")";
    return;
  }

  // kStorageHDF5: poll filesystem, dispatch one file per handshake
  size_t idle_cnt = 0;

  while (m_running_flag && m_running_flag->load()) {
    // Serialize: wait for the current in-flight file to complete before scanning
    // for the next. Prevents TRs from multiple source files from interleaving
    // in FRW's prebuf and corrupting per-source-file bookkeeping.
    {
      std::unique_lock<std::mutex> lk(m_in_flight_mtx);
      if (!m_in_flight_files.empty()) {
        m_in_flight_cv.wait_for(lk, std::chrono::milliseconds(500));
        continue;
      }
    }

    auto files = get_hdf5files_from_storage();

    if (files.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      ++idle_cnt;
      if (idle_cnt % 120 == 0)
        TLOG() << "IDLE: no new HDF5 files after " << (idle_cnt * 500 / 1000)
               << " s.";
      continue;
    }

    idle_cnt = 0;

    for (auto &file : files) {
      if (!(m_running_flag && m_running_flag->load()))
        break;

      m_input_h5_filename = file;

      // Skip files already in-flight (dispatched but WriteJSON deferred)
      {
        std::lock_guard<std::mutex> lk(m_in_flight_mtx);
        if (m_in_flight_files.count(m_input_h5_filename)) {
          TLOG() << "TRD: " << m_input_h5_filename
                 << " already in flight, skipping";
          continue;
        }
      }
      // Skip files within the write-fail backoff window
      {
        std::lock_guard<std::mutex> blk(m_backoff_mtx);
        auto it = m_backoff_files.find(m_input_h5_filename);
        if (it != m_backoff_files.end()) {
          if (std::chrono::steady_clock::now() < it->second) {
            TLOG_DEBUG(7) << "TRD: " << m_input_h5_filename
                          << " in write-fail backoff, skipping";
            continue;
          }
          m_backoff_files.erase(it);
          TLOG() << "TRD: backoff expired for "
                 << m_input_h5_filename << ", retrying";
        }
      }
      // Insert into in-flight -- double-check after the backoff lock gap
      {
        std::lock_guard<std::mutex> lk(m_in_flight_mtx);
        if (m_in_flight_files.count(m_input_h5_filename))
          continue;
        m_in_flight_files.insert(m_input_h5_filename);
      }

      TLOG() << "Dispatching from " << m_storage_pathname << " file "
             << m_input_h5_filename;

      receive(mode);
      break;  // one file per scan; outer loop re-enters only when in-flight is empty
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void TRDispatcher::do_stop(const data_t &) {
  TLOG() << "TRD do_stop() called, m_keep_running was="
         << m_keep_running.load();
  // Signal receive() to wake up and return; the WorkerThread loop uses
  // m_running_flag (not m_keep_running) so this only unblocks the CV wait.
  m_keep_running.store(false);
  m_start_barrier.store(false);
  m_req_prebuf_cv.notify_all();
  m_gen_window_cv.notify_all();

  // Stop the WorkerThread (which is running get_from_storage() -> receive()).
  // This sets running_flag=false and joins the thread.
  m_thread.stop_working_thread();

  // Now safe to clean up callbacks — no thread is using them anymore.
  {
    std::lock_guard<std::mutex> lk(m_req_prebuf_mtx);
    while (!m_req_prebuf.empty())
      m_req_prebuf.pop();
  }
  if (m_req_rx) {
    m_req_rx->remove_callback();
    m_req_rx.reset();
  }

  // Clean up BK callback and waiters after the worker thread has exited.
  if (m_bk_always_on_rx) {
    m_bk_always_on_rx->remove_callback();
    m_bk_always_on_rx.reset();
  }
  {
    std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
    m_bk_waiters.clear();
  }
}

void TRDispatcher::do_work(std::atomic<bool> &running_flag) {
  TLOG() << "TRD do_work: FIRST LINE running_flag=" << running_flag.load()
         << " addr=" << &running_flag;
  m_running_flag = &running_flag;

  // Two-phase barrier to prevent lost wakeup between WorkerThread and
  // do_start(). Phase 1: WorkerThread signals m_worker_ready before waiting.
  // Phase 2: do_start() waits for m_worker_ready, then sets m_start_barrier,
  //          then notifies the CV.  This guarantees the notification is not
  //          lost.
  // In standalone mode (no downstream), do_start() won't set m_start_barrier,
  // so we use a timeout and check m_keep_running.
  TLOG()
      << "TRD do_work: signaling m_worker_ready=true, then waiting on barrier";
  {
    std::unique_lock<std::mutex> lk(m_req_prebuf_mtx);
    m_worker_ready.store(true); // Signal BEFORE waiting
    TLOG() << "TRD do_work: m_worker_ready set, now waiting for barrier...";
    bool barrier_set = m_req_prebuf_cv.wait_for(
        lk, std::chrono::seconds(2), [this] { return m_start_barrier.load(); });
    if (barrier_set) {
      TLOG() << "TRD do_work: start barrier SET (do_start() completed, barrier "
                "received)";
    } else {
      TLOG() << "TRD do_work: start barrier TIMEOUT (m_worker_ready="
             << m_worker_ready.load()
             << " m_keep_running=" << m_keep_running.load() << ")";
      if (m_keep_running.load()) {
        TLOG() << "TRD do_work: m_keep_running=1, proceeding (standalone mode)";
      } else {
        TLOG() << "TRD do_work: m_keep_running=0, exiting";
        m_running_flag = nullptr;
        return;
      }
    }
  }

  TLOG() << "TRD do_work: entering (m_is_from_storage=" << m_is_from_storage
         << " running_flag=" << running_flag.load() << " addr=" << &running_flag
         << " m_running_flag=" << m_running_flag << ")";
  get_from_storage();
  m_running_flag = nullptr;
  TLOG() << "TRD do_work: get_from_storage() returned";
}

void TRDispatcher::generate_opmon_data() {
  dunedaq::datafilter::opmon::TRDispatcherInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

// Receive handshake from FilterOrchestrator
void TRDispatcher::receive(DispatchMode mode) {
  TLOG_DEBUG(7) << "receive(): enter, m_keep_running=" << m_keep_running.load();
  // Drain from the always-on prebuf (registered in do_start()).
  // Replaces transient add_callback/remove_callback to prevent between-cycle
  // drops.
  {
    std::unique_lock<std::mutex> lk(m_req_prebuf_mtx);
    m_req_prebuf_cv.wait_for(lk, std::chrono::milliseconds(100), [this] {
      return !m_req_prebuf.empty() || !m_keep_running.load();
    });
    if (!m_keep_running.load()) {
      TLOG_DEBUG(7) << "receive(): m_keep_running is false, returning";
      return;
    }
    if (m_req_prebuf.empty()) {
      TLOG_DEBUG(7) << "receive(): timeout, no request yet, returning to retry";
      // Release in-flight guard so the file is retried on the next scan.
      {
        std::lock_guard<std::mutex> lk(m_in_flight_mtx);
        m_in_flight_files.erase(m_input_h5_filename);
      }
      return;
    }
    TLOG_DEBUG(7) << "receive(): got request from prebuf, size="
                  << m_req_prebuf.size();
    m_req_prebuf.pop();
  }

  switch (mode) {
  case DispatchMode::kStorageHDF5: {
    bool tr_owns = false, ts_owns = false;
    // Not derivable from *_owns, which is false both for a clean skip (wrong
    // record type -- no retry wanted) and for a throw (retry wanted).
    bool tr_threw = false, ts_threw = false;
    {
      // An exception escaping a std::thread body terminates the process, and
      // HDF5RawDataFile's ctor throws FileOpenFailed for a missing or
      // unreadable path. Treat a throw as "file skipped": *_owns stays false,
      // so the in-flight guard is released below, per the contract documented
      // on send_tr_from_hdf5file().
      std::thread tr_th([&] {
        try {
          tr_owns = send_tr_from_hdf5file();
        } catch (const std::exception &e) {
          tr_threw = true;
          TLOG() << "TRD: TR dispatch failed for " << m_input_h5_filename
                 << ": " << e.what();
        }
      });
      std::thread ts_th([&] {
        try {
          ts_owns = send_ts_from_hdf5file();
        } catch (const std::exception &e) {
          ts_threw = true;
          TLOG() << "TRD: TS dispatch failed for " << m_input_h5_filename
                 << ": " << e.what();
        }
      });
      tr_th.join();
      ts_th.join();
    }
    if (!tr_owns && !ts_owns) {
      if (tr_threw || ts_threw) {
        // Reuse the existing retry window: without it a permanently bad path
        // is re-dispatched every loop pass (~10 Hz) and floods the log.
        std::lock_guard<std::mutex> blk(m_backoff_mtx);
        m_backoff_files[m_input_h5_filename] =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
      }
      std::lock_guard<std::mutex> lk(m_in_flight_mtx);
      m_in_flight_files.erase(m_input_h5_filename);
    }
    break;
  }

  case DispatchMode::kGeneratedSerial:
    if (m_generate_trigger_record)
      send_tr();
    if (m_generate_time_slice)
      send_ts();
    break;

  case DispatchMode::kGeneratedParallel: {
    std::thread tr_thread, ts_thread;

    if (m_generate_trigger_record)
      tr_thread = std::thread([this] { send_tr(); });

    if (m_generate_time_slice)
      ts_thread = std::thread([this] { send_ts(); });

    if (tr_thread.joinable())
      tr_thread.join();
    if (ts_thread.joinable())
      ts_thread.join();
    break;
  }

  } // switch
}

// generate a dummy test trigger record to be send to datafilter
trigger_record_ptr_t TRDispatcher::create_trigger_record(uint64_t trig_num) {
  std::vector<char> dummy_vector(fragment_size);

  for (auto &i : dummy_vector) {
    i = std::rand();
  }
  char *dummy_data = dummy_vector.data();

  // generate the timestamp for trigger record
  int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                   system_clock::now().time_since_epoch())
                   .count();

  // create TriggerRecordHeader
  dunedaq::daqdataformats::TriggerRecordHeaderData trh_data;
  trh_data.trigger_number = trig_num;
  trh_data.trigger_timestamp = ts;
  trh_data.num_requested_components = components_per_record;
  trh_data.run_number = run_number;
  trh_data.sequence_number = 0;
  trh_data.max_sequence_number = 1;
  trh_data.element_id = dunedaq::daqdataformats::SourceID(
      dunedaq::daqdataformats::SourceID::Subsystem::kTRBuilder, 0);

  dunedaq::daqdataformats::TriggerRecordHeader trh(&trh_data);

  // create our TriggerRecord
  auto tr = std::make_unique<dunedaq::daqdataformats::TriggerRecord>(trh);

  // loop over elements tpc
  for (size_t ele_num = 0; ele_num < element_count_tpc; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kWIBEth);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kHD_TPC);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
        ele_num);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  // loop over elements pds
  for (size_t ele_num = 0; ele_num < element_count_pds; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kDAPHNE);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kHD_PDS);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
        ele_num + element_count_tpc);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    // tr.add_fragment(std::move(frag_ptr));
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  // loop over TriggerActivity
  for (size_t ele_num = 0; ele_num < element_count_ta; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts - 10;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kTriggerActivity);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kDAQ);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kTrigger, ele_num);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  // loop over TriggerCandidate
  for (size_t ele_num = 0; ele_num < element_count_tc; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kTriggerCandidate);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kDAQ);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kTrigger,
        ele_num + element_count_ta);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  trigger_record_ptr_t temp = std::move(tr);
  return temp;
}

timeslice_ptr_t TRDispatcher::create_time_slice(uint64_t ts_num) {
  std::vector<char> dummy_vector(fragment_size);
  for (auto &i : dummy_vector)
    i = std::rand();
  char *dummy_data = dummy_vector.data();

  int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                   system_clock::now().time_since_epoch())
                   .count();

  dunedaq::daqdataformats::TimeSliceHeader tsh;
  tsh.timeslice_number = ts_num;
  tsh.run_number = run_number;
  tsh.element_id = dunedaq::daqdataformats::SourceID(
      dunedaq::daqdataformats::SourceID::Subsystem::kTRBuilder, 0);

  auto tslice = std::make_unique<dunedaq::daqdataformats::TimeSlice>(tsh);

  for (size_t ele_num = 0; ele_num < element_count_tpc; ++ele_num) {
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = ts_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kWIBEth);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kHD_TPC);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
        ele_num);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);
    tslice->add_fragment(std::move(frag_ptr));
  }

  for (size_t ele_num = 0; ele_num < element_count_pds; ++ele_num) {
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = ts_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kDAPHNE);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kHD_PDS);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
        ele_num + element_count_tpc);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);
    tslice->add_fragment(std::move(frag_ptr));
  }

  return tslice;
}

// send trigger records from self generated TR
void TRDispatcher::send_tr() {
  if (m_number_generated_events > 0) {
    auto prev = m_events_remaining.fetch_sub(1);
    if (prev == 0) {
      m_events_remaining.fetch_add(1);
      TLOG() << "send_tr: event limit (" << m_number_generated_events
             << ") reached, skipping";
      return;
    }
  }

  // Bounded in-flight window: block (with periodic wakeup) until fewer than
  // m_generated_window TR cycles are dispatched-but-unconfirmed. Bounded
  // wait, not unbounded wait(), so a stuck predicate cannot prevent
  // do_stop()'s stop_working_thread() from joining this thread. Placed
  // before trig_num is fetched so a blocked call never burns a sequence
  // number.
  {
    std::unique_lock<std::mutex> lk(m_gen_window_mtx);
    while (m_tr_in_flight.load(std::memory_order_acquire) >=
               static_cast<int>(m_generated_window) &&
           m_keep_running.load() && m_running_flag && m_running_flag->load())
      m_gen_window_cv.wait_for(lk, std::chrono::milliseconds(500));
    if (!m_keep_running.load() || !(m_running_flag && m_running_flag->load())) {
      TLOG() << "send_tr: shutting down, abandoning dispatch";
      return;
    }
  }

  std::ostringstream ss;
  auto trig_num = m_tr_seq_num.fetch_add(1);

  m_trdispatcher_id = m_cx.tr_data_tx.front();

  if (m_cx.tr_data_tx.empty()) {
    TLOG() << "No tr_data_tx discovered; skipping TR send.";
    return;
  }

  if (m_cx.tr_tracking_tx.empty()) {
    TLOG() << "No tr_tracking_tx discovered; Making sure that tracking is in "
              "the OKS file.";
    return;
  }

  // Send initial BK (kAssignedToFilter) before TR data so DF can open FRW's
  // dispatch gate. In HDF5 mode this happens in send_tr_from_hdf5file();
  // generated mode must replicate it, otherwise FRW never registers its
  // trwriter0 / TR-data callbacks and the ctrl send times out.
  const uint64_t bk_seq = m_bk_seq.fetch_add(1);
  auto waiter = std::make_shared<CycleWaiter>();
  {
    std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
    m_bk_waiters[bk_seq] = waiter;
  }
  // Counter is incremented iff a waiter is registered, and decremented iff
  // that same waiter is erased (always-on BK callback) -- keeps the two in
  // lockstep across every early-return path below.
  m_tr_in_flight.fetch_add(1, std::memory_order_acq_rel);

  if (!m_bk_connection_o.empty()) {
    dunedaq::datafilter::time_point_to_string tp2s(
        dunedaq::datafilter::Precision::NANOSECONDS);
    dunedaq::datafilter::BookKeeping bk_gen(m_bk_connection_o);
    bk_gen.entry_id = tp2s(std::chrono::system_clock::now());
    bk_gen.from_id = "TRDispatcher";
    bk_gen.tr_status = to_string(TRStatus::kAssignedToFilter);
    bk_gen.run_number = run_number;
    // file_index groups generated_window consecutive TR cycles into one BK
    // JSON file (and, downstream, one shared file_index in FRW's HDF5 output
    // naming/metadata) instead of one per cycle -- trigger_number below
    // remains the unique per-record id.
    const uint64_t generated_window_size =
        m_generated_window > 0 ? m_generated_window : 1;
    bk_gen.file_attributes_info.push_back(
        {"file_index", std::to_string(trig_num / generated_window_size)});
    bk_gen.file_attributes_info.push_back({"record_type", "TR"});
    // Tells DF's bookkeeping manager how many cycles this file_index will
    // eventually cover, so it does not finalize the batch file just because
    // it happens to see zero cycles open at some intermediate moment -- new
    // cycles for the same file_index keep arriving until this many have been
    // minted (dispatch is sequential and paced by this same window, not
    // all-at-once).
    bk_gen.file_attributes_info.push_back(
        {"batch_size", std::to_string(generated_window_size)});
    // Marks this cycle as generated-mode dispatch so DF's bookkeeping
    // manager can disambiguate the filename from an independent TS-sequence
    // cycle that happens to share the same file_index (m_tr_seq_num and
    // m_ts_seq_num are independent counters). Storage mode never sets this.
    bk_gen.file_attributes_info.push_back({"dispatch_mode", "generated"});
    bk_gen.file_attributes_info.push_back(
        {"trigger_number", std::to_string(trig_num)});
    bk_gen.file_attributes_info.push_back(
        {"trd_bk_seq", std::to_string(bk_seq)});
    bk_gen.file_attributes_info.push_back({"total_tr", "1"});
    bk_gen.tr_header_info.push_back({"record size", "1"});
    try {
      auto bk_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              m_bk_connection_o);
      bk_sender->send(std::move(bk_gen), std::chrono::milliseconds(2000));
      TLOG() << "send_tr (generated): sent initial BK (seq=" << bk_seq << ")";
    } catch (const std::exception &e) {
      TLOG() << "send_tr (generated): initial BK send failed: " << e.what();
    }
  }

  auto init_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
      m_cx.tr_tracking_tx.front());

  dunedaq::datafilter::Handshake sent_t1("next_tr");
  sent_t1.total_tr = 1;
  init_sender->send(std::move(sent_t1), Sender::s_block);

  std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
  std::mutex tracking_mutex;

  auto info = std::make_shared<TRDispatcherInfo>(0, 0);
  trdispatchers.push_back(info);

  TLOG_DEBUG(7) << "Getting publisher objects for each connection";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers), [=](std::shared_ptr<TRDispatcherInfo> info) {
        auto before_sender = std::chrono::steady_clock::now();

        info->sender =
            dunedaq::get_iom_sender<trigger_record_ptr_t>(m_trdispatcher_id);
        auto after_sender = std::chrono::steady_clock::now();
        info->get_sender_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_sender - before_sender);
      });

  // On the first TR dispatch after each start/restart, wait for FRW's ZMQ
  // SUB socket to reconnect before publishing -- otherwise the very first
  // publish can be dropped (ZMQ "slow joiner"). One-time cost; cleared
  // immediately. Separate from m_pub_warmup_needed (storage mode) since TR
  // and TS use independent connections here.
  if (m_tr_pub_warmup_needed.exchange(false)) {
    TLOG() << "TRD: kPubSub warmup wait (200 ms) for TR subscriber reconnection";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TLOG_DEBUG(7) << "Starting publish threads";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers),
      [=, &completed_receiver_tracking,
       &tracking_mutex](std::shared_ptr<TRDispatcherInfo> info) {
        info->send_thread.reset(new std::thread(
            [=, &completed_receiver_tracking, &tracking_mutex]() {
              bool complete_received = false;

              while (!complete_received) {
                TLOG() << "Sender message: generate trigger "
                          "record";
                trigger_record_ptr_t temp_record(
                    create_trigger_record(trig_num));

                TLOG() << "Start sending  trigger record";
                info->sender->try_send(
                    std::move(temp_record),
                    std::chrono::milliseconds(m_send_timeout_ms));
                TLOG() << "End sending trigger record";
                ++info->messages_sent;
                {
                  std::lock_guard<std::mutex> lk(tracking_mutex);
                  if ((completed_receiver_tracking.count(info->group_id) &&
                       completed_receiver_tracking[info->group_id].count(
                           info->conn_id)) ||
                      completed_receiver_tracking.count(-1)) {
                    TLOG() << "Complete_received";
                    complete_received = true;
                  }
                }
                complete_received = true;
                break;
              } // while loop
            }));
      });

  TLOG_DEBUG(7) << "Joining send threads";
  for (auto &sender : trdispatchers) {
    sender->send_thread->join();
    sender->send_thread.reset(nullptr);
  }
  trdispatchers.clear();

  TLOG() << "TR send done; it will start the next send.";
}

// Send a generated TimeSlice (no HDF5 source).
void TRDispatcher::send_ts() {
  if (m_number_generated_events > 0) {
    auto prev = m_events_remaining.fetch_sub(1);
    if (prev == 0) {
      m_events_remaining.fetch_add(1);
      TLOG() << "send_ts: event limit (" << m_number_generated_events
             << ") reached, skipping";
      return;
    }
  }

  // Bounded in-flight window: same reasoning as send_tr(), gated
  // independently on the TS counter. Placed before ts_num is fetched so a
  // blocked call never burns a sequence number.
  {
    std::unique_lock<std::mutex> lk(m_gen_window_mtx);
    while (m_ts_in_flight.load(std::memory_order_acquire) >=
               static_cast<int>(m_generated_window) &&
           m_keep_running.load() && m_running_flag && m_running_flag->load())
      m_gen_window_cv.wait_for(lk, std::chrono::milliseconds(500));
    if (!m_keep_running.load() || !(m_running_flag && m_running_flag->load())) {
      TLOG() << "send_ts: shutting down, abandoning dispatch";
      return;
    }
  }

  if (m_cx.ts_data_tx.empty()) {
    TLOG() << "No ts_data_tx discovered; skipping TS send.";
    return;
  }
  m_tsdispatcher_id = m_cx.ts_data_tx.front();

  auto ts_sender = dunedaq::get_iom_sender<timeslice_ptr_t>(m_tsdispatcher_id);
  auto ts_num = m_ts_seq_num.fetch_add(1);

  // Send initial BK so DF opens FRW's dispatch gate before TS data arrives.
  const uint64_t ts_bk_seq = m_bk_seq.fetch_add(1);
  auto ts_waiter = std::make_shared<CycleWaiter>();
  ts_waiter->is_ts_waiter = true;
  {
    std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
    m_bk_waiters[ts_bk_seq] = ts_waiter;
  }
  m_ts_in_flight.fetch_add(1, std::memory_order_acq_rel);

  if (!m_bk_connection_o.empty()) {
    dunedaq::datafilter::time_point_to_string tp2s(
        dunedaq::datafilter::Precision::NANOSECONDS);
    dunedaq::datafilter::BookKeeping bk_gen(m_bk_connection_o);
    bk_gen.entry_id = tp2s(std::chrono::system_clock::now());
    bk_gen.from_id = "TRDispatcher";
    bk_gen.tr_status = to_string(TRStatus::kAssignedToFilter);
    bk_gen.run_number = run_number;
    // file_index groups generated_window consecutive TS cycles into one BK
    // JSON file -- mirrors send_tr(); ts_number below remains the unique
    // per-record id.
    const uint64_t generated_window_size =
        m_generated_window > 0 ? m_generated_window : 1;
    bk_gen.file_attributes_info.push_back(
        {"file_index", std::to_string(ts_num / generated_window_size)});
    bk_gen.file_attributes_info.push_back({"record_type", "TS"});
    bk_gen.file_attributes_info.push_back({"dispatch_mode", "generated"});
    // See send_tr()'s batch_size comment.
    bk_gen.file_attributes_info.push_back(
        {"batch_size", std::to_string(generated_window_size)});
    bk_gen.file_attributes_info.push_back({"total_tr", "1"});
    bk_gen.file_attributes_info.push_back(
        {"ts_number", std::to_string(ts_num)});
    bk_gen.file_attributes_info.push_back(
        {"trd_bk_seq", std::to_string(ts_bk_seq)});
    bk_gen.tr_header_info.push_back({"record size", "1"});
    try {
      auto bk_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              m_bk_connection_o);
      bk_sender->send(std::move(bk_gen), std::chrono::milliseconds(2000));
      TLOG() << "send_ts (generated): sent initial BK (seq=" << ts_bk_seq
             << ")";
    } catch (const std::exception &e) {
      TLOG() << "send_ts (generated): initial BK send failed: " << e.what();
    }
  }

  // Tell DataFilter how many TSs to expect so it can send "write_ts" to FRW.
  if (!m_cx.tr_tracking_tx.empty()) {
    try {
      auto hs_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
          m_cx.tr_tracking_tx.front());
      dunedaq::datafilter::Handshake hs("next_ts");
      hs.total_tr = 1;
      hs_sender->send(std::move(hs), Sender::s_block);
      TLOG() << "send_ts (generated): sent Handshake next_ts total=1";
    } catch (const std::exception &e) {
      TLOG() << "send_ts (generated): next_ts handshake failed: " << e.what();
    }
  }

  // See send_tr()'s equivalent comment; TS uses its own connection and flag.
  if (m_ts_pub_warmup_needed.exchange(false)) {
    TLOG() << "TRD: kPubSub warmup wait (200 ms) for TS subscriber reconnection";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TLOG() << "Sending generated TimeSlice " << ts_num;
  ts_sender->try_send(create_time_slice(ts_num),
                      std::chrono::milliseconds(m_send_timeout_ms));
  TLOG() << "TS send done.";
}

// Send trigger records from generated hdf5 files.
// Returns true if a BK waiter was registered (guard ownership transferred to
// the always-on BK callback); false if the file was skipped or config is bad
// (caller is responsible for releasing the in-flight guard).
bool TRDispatcher::send_tr_from_hdf5file() {
  // Previous threads were already joined at end of last call; clear the
  // vector so this call spawns exactly ONE send thread instead of N on the
  // N-th cycle.
  trdispatchers.clear();

  std::ostringstream oss;

  // m_trdispatcher_id = "conn_A0_G0_C0_"; // to get it from config.

  if (!m_cx.tr_data_tx.empty()) {
    m_trdispatcher_id = m_cx.tr_data_tx.front();
  } else {
    throw std::runtime_error(
        "No TriggerRecord TX connection discovered (tr_data_tx is empty)");
  }

  HDF5RawDataFile h5_file(m_input_h5_filename);
  if (!h5_file.is_trigger_record_type()) {
    TLOG_DEBUG(7) << "File " << m_input_h5_filename
                  << " is not a TriggerRecord file (record_type="
                  << h5_file.get_record_type() << "); skipping TR send.";
    return false;
  }
  auto records = h5_file.get_all_trigger_record_ids();
  if (records.empty()) {
    TLOG() << "No TriggerRecords in " << m_input_h5_filename;
    return false;
  }
  auto records_size = records.size();
  auto total_tr = *(std::next(records.begin(), records.size() - 1));
  oss << "Last trigger record: " << int(total_tr.first) << ","
      << total_tr.second << "\n";

  TLOG() << oss.str();
  oss.str("");
  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  auto t1 = std::chrono::system_clock::now();
  dunedaq::datafilter::BookKeeping bk_info(m_bk_connection_o);
  bk_info.entry_id = time_point_to_string(t1);
  bk_info.conn_id = m_bk_info_id;
  bk_info.from_id = "trdispatcher";
  dunedaq::datafilter::node_info node_info;
  bk_info.node = node_info.get_node_info();

  bk_info.tr_header_info.push_back(
      {"record size", std::to_string(records.size())});
  auto file_index = h5_file.get_attribute<size_t>("file_index");
  TLOG() << "File index :" << file_index;
  bk_info.run_number = h5_file.get_attribute<size_t>("run_number");
  bk_info.file_attributes_info.push_back(
      {"file_index", std::to_string(file_index)});
  bk_info.file_attributes_info.push_back({"record_type", "TR"});
  const uint64_t h5_bk_seq = m_bk_seq.fetch_add(1);
  bk_info.file_attributes_info.push_back(
      {"trd_bk_seq", std::to_string(h5_bk_seq)});
  bk_info.file_attributes_info.push_back(
      {"total_tr", std::to_string(records.size())});
  bk_info.file_send_list.push_back(m_input_h5_filename);
  bk_info.tr_status = to_string(TRStatus::kAssignedToFilter);

  // Fully initialize the waiter before inserting into the map so that
  // the always-on BK callback never sees a partially-constructed entry
  // even when FRW responds before the send thread finishes.
  auto h5_waiter = std::make_shared<CycleWaiter>();
  h5_waiter->is_hdf5_mode = true;
  h5_waiter->h5_filename = m_input_h5_filename;
  h5_waiter->storage_pathname = m_storage_pathname;
  h5_waiter->json_file = m_json_file;
  h5_waiter->file_send_list = {m_input_h5_filename};
  {
    std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
    m_bk_waiters[h5_bk_seq] = h5_waiter;
  }

  if (m_bk_connection_o.empty()) {
    throw std::runtime_error(
        "No bookkeeping TX connection discovered (bk_outputs is empty)");
  }
  auto bookkeeping_sender =
      dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
          m_bk_connection_o);

  bookkeeping_sender->send(std::move(bk_info), std::chrono::milliseconds(2000));

  if (m_cx.tr_tracking_tx.empty()) {
    TLOG() << "TR_tracking2 to DF is empty.";
    return false;
  }
  TLOG() << "m_cx.tr_tracking_tx " << m_cx.tr_tracking_tx.front();
  // Handshake with datafilter.
  auto init_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
      m_cx.tr_tracking_tx.front());

  dunedaq::datafilter::Handshake sent_t1("next_tr");
  // send total trigger number to datafilter then datafilter to
  // FilterResultWriter
  sent_t1.total_tr = int(records_size);

  init_sender->send(std::move(sent_t1), Sender::s_block);

  std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
  std::mutex tracking_mutex;

  // for (size_t group = 0; group < config.num_groups; ++group) {
  //     for (size_t conn = 0; conn < config.num_connections_per_group;
  //          ++conn) {

  // auto info = std::make_shared<TRDispatcherInfo>(group, conn);
  auto info = std::make_shared<TRDispatcherInfo>(0, 0);
  trdispatchers.push_back(info);
  //  }
  // }

  TLOG_DEBUG(7) << "Getting publisher objects for each connection";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers), [=](std::shared_ptr<TRDispatcherInfo> info) {
        auto before_sender = std::chrono::steady_clock::now();
        info->sender =
            dunedaq::get_iom_sender<trigger_record_ptr_t>(m_trdispatcher_id);
        auto after_sender = std::chrono::steady_clock::now();
        info->get_sender_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_sender - before_sender);
      });

  // On the first dispatch after each start/restart, wait for ZMQ SUB sockets
  // to reconnect before publishing.  One-time cost; cleared immediately.
  if (m_pub_warmup_needed.exchange(false)) {
    TLOG() << "TRD: kPubSub warmup wait (200 ms) for subscriber reconnection";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TLOG_DEBUG(7) << "Starting publish threads";
  // Set by any send thread that could not read the file. Atomic because
  // trdispatchers may hold several senders; read only after all joins.
  std::atomic<bool> h5_send_failed{false};
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers),
      [=, &bk_info, &h5_file, &completed_receiver_tracking, &tracking_mutex,
       &h5_send_failed](std::shared_ptr<TRDispatcherInfo> info) {
        info->send_thread.reset(new std::thread([=, &bk_info, &h5_file,
                                                 &completed_receiver_tracking,
                                                 &tracking_mutex,
                                                 &h5_send_failed]() {
          bool complete_received = false;
          bool all_sends_ok = true;

          std::ostringstream oss;
          while (!complete_received) {
            TLOG() << "Sender message: trigger record";

            HDF5RawDataFile::record_id_set records;
            try {
              records = h5_file.get_all_trigger_record_ids();
            } catch (const std::exception &e) {
              TLOG() << "TRD: get_all_trigger_record_ids failed for "
                     << m_input_h5_filename << ": " << e.what();
              h5_send_failed.store(true);
              break;
            }
            oss << "\nNumber of TriggerRecords: " << records.size();
            if (records.empty()) {
              oss << "\n\nNO TRIGGER RECORDS FOUND";
              TLOG() << oss.str();
              break;
            }
            auto first_rec = *(records.begin());
            auto last_rec = *(std::next(records.begin(), records.size() - 1));

            oss << "\n\tFirst trigger record: " << first_rec.first << ","
                << first_rec.second;
            oss << "\n\tLast trigger record: " << last_rec.first << ","
                << last_rec.second;

            TLOG() << oss.str();
            oss.str("");

            for (auto const &rid : records) {
              try {
                auto tr = h5_file.get_trigger_record(rid);

                if (tr.get_fragments_ref().empty()) {
                  TLOG() << "TR " << rid.first << "," << rid.second
                         << " has no fragments, skipping.";
                  all_sends_ok = false;
                  continue;
                }

                m_trigger_number =
                    tr.get_fragments_ref().at(0)->get_trigger_number();
                m_run_number = tr.get_fragments_ref().at(0)->get_run_number();
                TLOG() << "Trigger number " << m_trigger_number
                       << " run_number " << m_run_number;
                // SERIALIZE
                auto bytes = dunedaq::serialization::serialize(
                    tr, dunedaq::serialization::kMsgPack);
                // DESERIALIZE
                auto deserialized =
                    dunedaq::serialization::deserialize<trigger_record_ptr_t>(
                        bytes);

                try {
                  info->sender->try_send(std::move(deserialized),
                                         std::chrono::milliseconds(50));
                } catch (const std::exception &e) {
                  TLOG() << "try_send failed for trigger record " << rid.first
                         << "," << rid.second << ": " << e.what()
                         << " — will not mark source file as "
                            "transferred.";
                  all_sends_ok = false;
                }
              } catch (const std::exception &e) {
                TLOG() << "get_trigger_record failed for rid " << rid.first
                       << "," << rid.second << ": " << e.what()
                       << " — skipping this TR, will not mark "
                          "file transferred.";
                all_sends_ok = false;
              }
            }

            ++info->messages_sent;
            {
              std::lock_guard<std::mutex> lk(tracking_mutex);
              if ((completed_receiver_tracking.count(info->group_id) &&
                   completed_receiver_tracking[info->group_id].count(
                       info->conn_id)) ||
                  completed_receiver_tracking.count(-1)) {
                TLOG() << "Complete_received";
                complete_received = true;
              }
            }
            complete_received = true;
            break;
          } // while loop

          TLOG() << "TRD: HDF5 TR send thread done for " << m_input_h5_filename
                 << " (seq=" << h5_bk_seq << ")";
          ++info->messages_sent;
        }));
      });

  TLOG() << "Joining send threads";
  for (auto &sender : trdispatchers) {
    sender->send_thread->join();
    sender->send_thread.reset(nullptr);
  }

  if (h5_send_failed.load()) {
    // No TR was sent, so FRW will never confirm and the always-on BK callback
    // would never reap this waiter or release the in-flight guard. Drop the
    // waiter and report failure, letting the caller release the guard and
    // apply the retry backoff.
    {
      std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
      m_bk_waiters.erase(h5_bk_seq);
    }
    TLOG() << "send_tr_from_hdf5file: read failed for " << m_input_h5_filename
           << ", dropped BK waiter (seq=" << h5_bk_seq << ")";
    return false;
  }
  return true;
}

// Send TimeSlices from the same HDF5 file.
// Returns true if a BK waiter was registered (guard ownership transferred to
// the always-on BK callback); false if the file was skipped.
bool TRDispatcher::send_ts_from_hdf5file() {
  if (m_cx.ts_data_tx.empty()) {
    TLOG_DEBUG(7)
        << "No TimeSlice TX connections configured; skipping TS send.";
    return false;
  }
  m_tsdispatcher_id = m_cx.ts_data_tx.front();

  HDF5RawDataFile h5_file(m_input_h5_filename);
  if (!h5_file.is_timeslice_type()) {
    TLOG_DEBUG(7) << "File " << m_input_h5_filename
                  << " is not a TimeSlice file (record_type="
                  << h5_file.get_record_type() << "); skipping TS send.";
    return false;
  }
  auto ts_records = h5_file.get_all_timeslice_ids();
  if (ts_records.empty()) {
    TLOG_DEBUG(7) << "No TimeSlices in " << m_input_h5_filename;
    return false;
  }

  TLOG() << "Sending " << ts_records.size() << " TimeSlice(s) from "
         << m_input_h5_filename;

  dunedaq::datafilter::time_point_to_string tp2s(
      dunedaq::datafilter::Precision::NANOSECONDS);
  const size_t ts_run_number = h5_file.get_attribute<size_t>("run_number");
  const size_t ts_file_index = h5_file.get_attribute<size_t>("file_index");

  const uint64_t ts_h5_bk_seq = m_bk_seq.fetch_add(1);
  // Fully initialize before map insertion -- same race guard as TR path.
  auto ts_h5_waiter = std::make_shared<CycleWaiter>();
  ts_h5_waiter->is_hdf5_mode = true;
  ts_h5_waiter->h5_filename = m_input_h5_filename;
  ts_h5_waiter->storage_pathname = m_storage_pathname;
  ts_h5_waiter->json_file = m_json_file;
  ts_h5_waiter->file_send_list = {m_input_h5_filename};
  {
    std::lock_guard<std::mutex> lk(m_bk_waiters_mtx);
    m_bk_waiters[ts_h5_bk_seq] = ts_h5_waiter;
  }

  // Send initial BK (kAssignedToFilter) to open FRW's dispatch gate.
  if (!m_bk_connection_o.empty()) {
    dunedaq::datafilter::BookKeeping init_bk(m_bk_connection_o);
    init_bk.entry_id = tp2s(std::chrono::system_clock::now());
    init_bk.from_id = "trdispatcher";
    init_bk.run_number = ts_run_number;
    init_bk.file_attributes_info.push_back(
        {"file_index", std::to_string(ts_file_index)});
    init_bk.file_attributes_info.push_back({"record_type", "TS"});
    init_bk.file_attributes_info.push_back(
        {"total_tr", std::to_string(ts_records.size())});
    init_bk.file_attributes_info.push_back(
        {"trd_bk_seq", std::to_string(ts_h5_bk_seq)});
    init_bk.tr_header_info.push_back(
        {"record size", std::to_string(ts_records.size())});
    init_bk.file_send_list.push_back(m_input_h5_filename);
    init_bk.tr_status = to_string(TRStatus::kAssignedToFilter);
    try {
      auto bk_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              m_bk_connection_o);
      bk_sender->send(std::move(init_bk), std::chrono::milliseconds(2000));
      TLOG() << "send_ts_from_hdf5file: sent initial BK (seq=" << ts_h5_bk_seq
             << ")";
    } catch (const std::exception &e) {
      TLOG() << "send_ts_from_hdf5file: initial BK send failed: " << e.what();
    }
  }

  // Tell DataFilter how many TSs to expect so it can send "write_ts" to FRW.
  if (!m_cx.tr_tracking_tx.empty()) {
    try {
      auto hs_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
          m_cx.tr_tracking_tx.front());
      dunedaq::datafilter::Handshake hs("next_ts");
      hs.total_tr = static_cast<int>(ts_records.size());
      hs_sender->send(std::move(hs), Sender::s_block);
      TLOG() << "send_ts_from_hdf5file: sent next_ts total="
             << ts_records.size();
    } catch (const std::exception &e) {
      TLOG() << "send_ts_from_hdf5file: next_ts send failed: " << e.what();
    }
  }

  auto ts_sender = dunedaq::get_iom_sender<timeslice_ptr_t>(m_tsdispatcher_id);

  for (const auto &rid : ts_records) {
    auto ts = h5_file.get_timeslice(rid);
    TLOG() << "TimeSlice number " << rid.first << " sequence " << rid.second;

    auto bytes =
        dunedaq::serialization::serialize(ts, dunedaq::serialization::kMsgPack);
    auto deserialized =
        dunedaq::serialization::deserialize<timeslice_ptr_t>(bytes);

    ts_sender->try_send(std::move(deserialized),
                        std::chrono::milliseconds(m_send_timeout_ms));
  }

  TLOG() << "TimeSlice send done for " << m_input_h5_filename;

  TLOG() << "TRD: TS HDF5 send done for " << m_input_h5_filename
         << " (seq=" << ts_h5_bk_seq << ")";
  return true;
}

std::vector<std::filesystem::path> TRDispatcher::get_hdf5files_from_storage() {
  TLOG_DEBUG(7) << "I am in get_hdf5files_from_storage : storage_pathname"
                << m_storage_pathname << " json_file " << m_json_file;

  dunedaq::datafilter::HDF5FromStorage s(m_storage_pathname, m_json_file);
  // s.print();

  // for (auto file : s.hdf5_files_to_transfer) {
  //     std::cout << "main: files to transfer" << file << "\n";
  // }
  return s.hdf5_files_to_transfer;
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::TRDispatcher)
