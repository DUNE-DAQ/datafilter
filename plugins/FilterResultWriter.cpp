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

#include <optional>

using dunedaq::datafilter::FilterResultWriter;

namespace dunedaq::datafilter {

FilterResultWriter::FilterResultWriter(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(
          std::bind(&FilterResultWriter::do_work, this, std::placeholders::_1)),
      m_bk_thread(std::bind(&FilterResultWriter::receive_attrs, this,
                            std::placeholders::_1)) {
  register_command("conf", &FilterResultWriter::do_conf);
  register_command("start", &FilterResultWriter::do_start);
  register_command("stop", &FilterResultWriter::do_stop);
}

void FilterResultWriter::FilterResultWriter::init(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();

  dunedaq::conffwk::Configuration *confdb;

  try {
    confdb = new conffwk::Configuration(m_oksConfig);

  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  confdb->get<dunedaq::confmodel::Queue>(m_queues);
  confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);
}

void FilterResultWriter::do_conf(const data_t &) {
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

  TLOG() << get_name() << ": exist do_conf()";
}

void FilterResultWriter::do_start(const data_t &) {
  // TLOG() << get_name() << " do_start()";
  // m_bk_thread.start_working_thread();
  // m_thread.start_working_thread();
  // TLOG() << get_name() << ": exist do_start()";

  start_attrs_test_thread();
  //  receive_tr(0);
}
void FilterResultWriter::do_stop(const data_t & /* do not pass an argument*/) {
  TLOG() << get_name() << " do_stop()";
  stop_attrs_test_thread();
  // m_thread.stop_working_thread();
  // m_bk_thread.start_working_thread();

  TLOG() << get_name() << ": exist do_stop()";
}

void FilterResultWriter::do_work(std::atomic<bool> &running) {
  while (1) {
    receive_tr(0);
  }
}
// void FilterResultWriter::attrs_thread(std::atomic<bool> &running) {
//   while (1) {
//     receive_attrs();
//   }
// }
void FilterResultWriter::attrs_test_loop() {
  using BK = dunedaq::datafilter::BookKeeping;

  TLOG() << "attrs_test_loop: starting std::thread receiver";

  auto receiver = dunedaq::get_iom_receiver<BK>("bookkeeping1");
  // auto receiver = dunedaq::get_iom_receiver<Handshake>("trdispatcher0");
  if (!receiver) {
    TLOG() << "attrs_test_loop: failed to get 'bookkeeping1' receiver";
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
          TLOG() << "attrs_test_loop: invalid file_index '" << kv.second
                 << "' (" << e.what() << ")";
        }
        break;
      }
    }

    TLOG_DEBUG(1) << "BookKeeping from " << bk.from_id
                  << " run=" << bk.run_number
                  << " file_index=" << FilterResultWriter::get_file_index();
  };

  TLOG() << "attrs_test_loop: registering callback";
  receiver->add_callback(cb);

  // Keep callback alive for entire run; block here until stop requested
  {
    std::unique_lock<std::mutex> lk(m_attrs_test_mtx);
    m_attrs_test_cv.wait(lk, [this] {
      return !m_attrs_test_running.load(std::memory_order_relaxed);
    });
  }

  TLOG() << "attrs_test_loop: removing callback and exiting";
  receiver->remove_callback();
}

void FilterResultWriter::start_attrs_test_thread() {
  // prevent double-start
  bool was_running =
      m_attrs_test_running.exchange(true, std::memory_order_acq_rel);
  if (was_running)
    return;

  m_attrs_test_thread = std::thread(&FilterResultWriter::attrs_test_loop, this);
}

void FilterResultWriter::stop_attrs_test_thread() {
  bool was_running =
      m_attrs_test_running.exchange(false, std::memory_order_acq_rel);
  if (!was_running)
    return;

  // Wake the thread if it's waiting
  m_attrs_test_cv.notify_all();

  if (m_attrs_test_thread.joinable())
    m_attrs_test_thread.join();
}

std::string
FilterResultWriter::generate_hdf5file_pathname(std::string file_pathname_prefix,
                                               int run_number, int file_index,
                                               int trigger_number) {
  std::ostringstream filename_oss;
  filename_oss << file_pathname_prefix << "_" << std::setw(6)
               << std::setfill('0') << run_number << "_" << std::setw(4)
               << std::setfill('0') << file_index << "_" << trigger_number
               << ".hdf5";
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

// void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
//   TLOG() << "Receive attrs==================================";
//   std::atomic<unsigned int> received_cnt{0};
//   std::atomic<bool> is_done{false};

//   auto cb_receiver =
//       dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
//           "bookkeeping1");
//   if (!cb_receiver) {
//     TLOG() << "Failed to get bookkeeping receiver";
//     return;
//   }

//   std::function<void(dunedaq::datafilter::BookKeeping)> str_receiver_cb =
//       [&](dunedaq::datafilter::BookKeeping bk) {
//         ++received_cnt;
//         if (received_cnt == 1) {
//           if (auto it = std::find_if(
//                   bk.file_attributes_info.begin(),
//                   bk.file_attributes_info.end(),
//                   [](const auto &p) { return p.first == "file_index"; });
//               it != bk.file_attributes_info.end()) {
//             // m_file_index = it->second;
//             dunedaq::datafilter::FilterResultWriter::set_file_index(
//                 std::stoi(it->second));
//           }
//         }
//         TLOG() << "Processing bookkeeping attributes # " << received_cnt
//                << " from " << bk.from_id << " (Run: " << bk.run_number
//                << " file index: " << get_file_index() << ")";
//       };

//   TLOG() << "Registering callback...";
//   cb_receiver->add_callback(str_receiver_cb);
//   TLOG() << "Callback registered, entering main loop";
//   while (!is_done) {
//     if (received_cnt == 1) {
//       TLOG() << "Check received_cnt" << received_cnt;
//       is_done = true;
//     }
//   }
//   TLOG() << "Cleaning up receiver";
//   cb_receiver->remove_callback();
// }

// void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
//   TLOG() << "BookKeeping attrs_thread starting";

//   auto cb_receiver =
//       dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
//           "bookkeeping1");
//   if (!cb_receiver) {
//     TLOG() << "Failed to get bookkeeping receiver";
//     return;
//   }

//   // Keep the callback tiny: parse + store
//   auto cb =
//       [this](dunedaq::datafilter::BookKeeping bkex(bk)) {
//         // If you have set_file_index()/get_file_index() methods, ensure
//         they
//         // use atomics internally.
//         s_file_index.store(*idx, std::memory_order_release);
//       }

//       TLOG_DEBUG(1)
//       << "BookKeeping from " << bk.from_id << " run=" << bk.run_number
//       << " file_index=" << s_file_index.load(std::memory_order_acquire);
// };

// TLOG() << "Registering bookkeeping callback";
// cb_receiver->add_callback(cb);

// // Stay alive for the entire run; don’t spin—sleep a little and check
// 'running' while (running.load(std::memory_order_relaxed)) {
//   std::this_thread::sleep_for(std::chrono::milliseconds(200));
// }

// TLOG() << "Removing bookkeeping callback";
// cb_receiver->remove_callback();

// TLOG() << "BookKeeping receive_attrs exiting";
// }

// If your set/get are not already thread-safe, back them with an atomic.
// (Comment this out if you already have thread-safe
// set_file_index/get_file_index.)
// static std::atomic<int> g_file_index_atomic{0};
// inline void set_file_index_atomic(int v) {
//   g_file_index_atomic.store(v, std::memory_order_release);
// }
// inline int get_file_index_atomic() {
//   return g_file_index_atomic.load(std::memory_order_acquire);
// }

void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
  TLOG() << "BookKeeping receive_attrs starting";

  auto receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
      "bookkeeping1");
  if (!receiver) {
    TLOG() << "Failed to get BookKeeping receiver 'bookkeeping1'";
    return;
  }

  std::function<void(dunedaq::datafilter::BookKeeping &)> cb =
      [&](dunedaq::datafilter::BookKeeping bk) {
        // Find "file_index" and update (keep this fast)
        for (const auto &kv : bk.file_attributes_info) {
          if (kv.first == "file_index") {
            try {
              const int idx = std::stoi(kv.second);
              FilterResultWriter::set_file_index(
                  idx); // <-- ensure this is thread-safe
            } catch (const std::exception &e) {
              TLOG() << "Invalid file_index value: '" << kv.second << "' ("
                     << e.what() << ")";
            }
            break;
          }
        }

        TLOG_DEBUG(1) << "BookKeeping from " << bk.from_id
                      << " run=" << bk.run_number
                      << " file_index=" << FilterResultWriter::get_file_index();
      };

  // std::function<void(dunedaq::datafilter::BookKeeping)> str_receiver_cb =
  //     [&](dunedaq::datafilter::BookKeeping bk) {
  //       ++received_cnt;
  //       if (received_cnt == 1) {
  //         if (auto it = std::find_if(
  //                 bk.file_attributes_info.begin(),
  //                 bk.file_attributes_info.end(),
  //                 [](const auto &p) { return p.first == "file_index"; });
  //             it != bk.file_attributes_info.end()) {
  //           // m_file_index = it->second;
  //           dunedaq::datafilter::FilterResultWriter::set_file_index(
  //               std::stoi(it->second));
  //         }
  //       }
  //       TLOG() << "Processing bookkeeping attributes # " << received_cnt
  //              << " from " << bk.from_id << " (Run: " << bk.run_number
  //              << " file index: " << get_file_index() << ")";
  //     };

  TLOG() << "Registering BookKeeping callback";
  receiver->add_callback(cb);

  // Keep callback alive for the entire run
  while (running.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TLOG() << "Removing BookKeeping callback";
  receiver->remove_callback();

  TLOG() << "BookKeeping attrs_thread exiting";
}

void FilterResultWriter::receive_tr(size_t run_number1) {
  bool handshake_done = false;
  std::atomic<unsigned int> received_cnt = 0;

  auto cb_receiver =
      dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
  std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        if (msg.msg_id == "write_tr") {
          ++received_cnt;
        }
        TLOG_DEBUG(5) << "FilterResultWriter: TR receiver callback: "
                      << msg.msg_id;
      };

  cb_receiver->add_callback(str_receiver_cb);
  while (!handshake_done) {
    if (received_cnt == 1)
      handshake_done = true;
  }

  //        if (config.next_tr) {
  //            auto next_tr_sender =
  //                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
  //                    "twriter0");
  //            TLOG() << "send next_tr instruction";
  //            dunedaq::datafilter::Handshake q("next_tr");
  //            next_tr_sender->send(std::move(q), Sender::s_block);
  //        }

  TLOG_DEBUG(5) << "Setting up TRWriterInfo objects";
  for (size_t group = 0; group < m_num_groups; ++group) {
    // trwriters.push_back(std::make_shared<TRWriterInfo>(group));
    for (size_t conn = 0; conn < m_num_connections_per_group; ++conn) {
      subscribers.push_back(std::make_shared<SubscriberInfo>(group, conn));
    }
  }

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);
  auto t1 = std::chrono::system_clock::now();
  dunedaq::datafilter::BookKeeping bk_info("bookkeeping0");
  bk_info.entry_id = time_point_to_string(t1);
  bk_info.conn_id = m_init_connection;
  bk_info.from_id = "FilterResultWriter";

  //        bk_info['entry_id'] = time_point_to_string(t1);
  //        bk_info['conn_id'] = config.get_connection_name(config.my_id,
  //        0, 0); bk_info['from_id'] = "FilterResultWriter";

  // the layout can be obtained from the tranfered TR.
  HDF5FileLayoutParameters fl_pars = create_file_layout_params();

  // create src-geo id map; this should be replaced the correct src-geo
  // map from the transfered TR.
  auto srcid_geoid_map = create_srcid_geoid_map();

  std::atomic<std::chrono::steady_clock::time_point> last_received =
      std::chrono::steady_clock::now();
  TLOG_DEBUG(5) << "Adding callbacks for each subscriber";
  std::for_each(
      std::execution::par_unseq, std::begin(subscribers), std::end(subscribers),
      [=, &last_received](std::shared_ptr<SubscriberInfo> info) {
        auto recv_proc = [=, &last_received](trigger_record_ptr_t &tr) {
          m_trigger_timestamp =
              tr->get_fragments_ref().at(0)->get_trigger_timestamp();
          m_trigger_number =
              tr->get_fragments_ref().at(0)->get_trigger_number();
          m_run_number = tr->get_fragments_ref().at(0)->get_run_number();
          size_t file_index = get_file_index();

          TLOG() << "run_number " << m_run_number
                 << " file index: " << file_index
                 << ", trigger number: " << m_trigger_number;
          info->msgs_received++;
          last_received = std::chrono::steady_clock::now();

          if (info->msgs_received == m_num_messages) {
            TLOG() << "Complete condition reached, sending "
                      "init message for "
                   << m_init_connection;
            std::string app_name = "test";
            // config.ofile_pathname =
            //     config.odir + "/" + config.output_h5_filename +
            //     "_" + std::to_string(config.run_number) + "_" +
            //     std::to_string(file_index) + "_" +
            //     std::to_string(config.trigger_number) + ".hdf5";

            std::string file_pathname_prefix =
                m_odir + "/" + m_output_h5_filename;
            m_ofile_pathname =
                generate_hdf5file_pathname(file_pathname_prefix, m_run_number,
                                           file_index, m_trigger_number);
            TLOG() << "Writing the TR to " << m_ofile_pathname;

            // create the file
            // std::unique_ptr<HDF5RawDataFile> h5file_ptr(new
            // HDF5RawDataFile(
            //     m_ofile_pathname, m_run_number, m_file_index, app_name,
            //     fl_pars, srcid_geoid_map, ".writing",
            //     HighFive::File::Overwrite));
            unsigned compression_level =
                0; // to move this to .hpp, selection from .data.xml

            std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
                m_ofile_pathname, m_run_number, m_file_index, app_name, fl_pars,
                srcid_geoid_map, compression_level));

            h5file_ptr->write(*tr);
            h5file_ptr.reset();
            info->complete = true;
          }
        };

        auto before_receiver = std::chrono::steady_clock::now();
        auto receiver = dunedaq::get_iom_receiver<
            std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
            m_init_connection);
        auto after_receiver = std::chrono::steady_clock::now();
        receiver->add_callback(recv_proc);
        auto after_callback = std::chrono::steady_clock::now();
        info->get_receiver_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_receiver - before_receiver);
        info->add_callback_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_callback - after_receiver);
      });

  // ACK to datafilter that we succefully received the TR
  TLOG() << "Send bookkeeping info to datafilter server";
  bk_info.tr_status = "received";
  bk_info.run_number = m_run_number;
  bk_info.tr_header_info.push_back(
      {"run_number", std::to_string(m_run_number)});
  bk_info.tr_header_info.push_back(
      {"trigger_number", std::to_string(m_trigger_number)});
  bk_info.tr_header_info.push_back({"tr_writer_pathname", m_ofile_pathname});

  auto init_bookkeeping_sender =
      dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>("bookkeeping0");
  init_bookkeeping_sender->send(std::move(bk_info), Sender::s_block);

  //        if (config.next_tr) {
  //            auto next_tr_sender =
  //                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
  //                    "trwriter0");
  //            TLOG() << "send wait for next instruction";
  //            dunedaq::datafilter::Handshake q("wait");
  //            next_tr_sender->send(std::move(q), Sender::s_block);
  //        }

  TLOG_DEBUG(5) << "Starting wait loop for receives to complete";
  bool all_done = false;
  while (!all_done) {
    size_t recvrs_done = 0;
    for (auto &sub : subscribers) {
      if (sub->complete.load())
        recvrs_done++;
    }
    TLOG_DEBUG(6) << "Done: " << recvrs_done << ", expected: "
                  << m_num_groups * m_num_connections_per_group;
    all_done = recvrs_done >= m_num_groups * m_num_connections_per_group;
    if (!all_done)
      std::this_thread::sleep_for(1ms);
  }
  TLOG_DEBUG(5) << "Removing callbacks";
  for (auto &info : subscribers) {
    auto receiver =
        dunedaq::get_iom_receiver<trigger_record_ptr_t>(m_init_connection);
    receiver->remove_callback();
  }

  auto cb_receiver1 =
      dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
  cb_receiver1->remove_callback();

  subscribers.clear();
  TLOG_DEBUG(5) << "receive() done";
}

void FilterResultWriter::send_next_tr(size_t run_number, pid_t subscriber_pid) {
  bool handshake_done = false;

  std::atomic<unsigned int> sent_cnt = 0;

  auto sender_next_tr =
      dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("trdispatcher1");

  // std::chrono::milliseconds timeout(100);
  dunedaq::datafilter::Handshake sent_t1("trdispatcher1");
  // sender_next_tr->send(std::move(sent_t1), timeout);
  sender_next_tr->send(std::move(sent_t1), Sender::s_block);
}

void FilterResultWriter::generate_opmon_data() {
  dunedaq::datafilter::opmon::FilterResultWriterInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterResultWriter)
