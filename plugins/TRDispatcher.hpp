/**
 * @file TRDispatcher.hpp
 *
 * Developer(s) of this DAQModule have yet to replace this line with a brief
 * description of the DAQModule.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef DATAFILTER_PLUGINS_TRDISPATCHER_HPP_
#define DATAFILTER_PLUGINS_TRDISPATCHER_HPP_

#include <atomic>
#include <condition_variable>
#include <execution>
#include <limits>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>
#include <unordered_set>
#include <unordered_map>

#include "appfwk/DAQModule.hpp"
#include "confmodel/DaqApplication.hpp"
#include "daqdataformats/TimeSlice.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeaderData.hpp"
#include "datafilter/HDF5FromStorage.hpp"
#include "datafilter/TimeSlice_serialization.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "datafilter/node_info.hpp"
#include "dfmessages/TriggerRecord_serialization.hpp"
#include "opmonlib/TestOpMonManager.hpp"
#include "utilities/WorkerThread.hpp"

#include "conffwk/ConfigObject.hpp"
#include "conffwk/ConfigObjectImpl.hpp"

#include "datafilter/core/Connections.hpp"
#include "datafilter/core/ConnectionsBuilder.hpp"
#include "datafilter/dal/TRDispatcher.hpp"
#include "datafilter/opmon/trdispatcher_info.pb.h"
#include "detdataformats/DetID.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"

using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;
using timeslice_ptr_t = std::unique_ptr<dunedaq::daqdataformats::TimeSlice>;

using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::detdataformats;
using namespace dunedaq::iomanager;

namespace dunedaq::datafilter {

class TRDispatcher : public dunedaq::appfwk::DAQModule {
public:
  struct TRDispatcherInfo {
    size_t conn_id;
    size_t group_id;
    size_t messages_sent = 0;
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t run_number = 53;
    size_t element_id;
    size_t detector_id;
    size_t error_bits;
    size_t fragment_type;
    std::string path_header;
    int n_frames;

    std::shared_ptr<SenderConcept<trigger_record_ptr_t>> sender;
    std::unique_ptr<std::thread> send_thread;
    std::chrono::milliseconds get_sender_time;

    TRDispatcherInfo(size_t group, size_t conn)
        : conn_id(conn), group_id(group) {}
  };

  explicit TRDispatcher(const std::string &name);

  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;
  void receive(DispatchMode mode);

  bool send_tr_from_hdf5file();
  bool send_ts_from_hdf5file();
  void send_tr();
  void send_ts();
  void get_from_storage();
  trigger_record_ptr_t create_trigger_record(uint64_t trig_num);
  timeslice_ptr_t create_time_slice(uint64_t ts_num);
  std::vector<std::filesystem::path> get_hdf5files_from_storage();

  std::vector<std::shared_ptr<TRDispatcherInfo>> trdispatchers;
  TRDispatcher(const TRDispatcher &) = delete;
  TRDispatcher &operator=(const TRDispatcher &) = delete;
  TRDispatcher(TRDispatcher &&) = delete;
  TRDispatcher &operator=(TRDispatcher &&) = delete;
  ~TRDispatcher() = default;

protected:
  void generate_opmon_data() override;

private:
  // Commands TRDispatcher can receive

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER
  // READING IT For any run control command it is possible for a DAQModule to
  // register an action that will be executed upon reception of the
  // command. do_conf is a very common example of this; in
  // TRDispatcher.cpp you would implement do_conf so that members of
  // TRDispatcher get assigned values from a configuration passed as
  // an argument and originating from the CCM system.

  void do_conf(const data_t &);
  void do_start(const data_t &);
  void do_stop(const data_t &);
  void do_work(std::atomic<bool> &running_flag);

  // In-flight file tracking: prevents re-dispatching the same HDF5 file
  // when WriteJSON is deferred to the BK callback (fire-and-forget).
  std::unordered_set<std::string> m_in_flight_files;
  std::mutex m_in_flight_mtx;
  std::condition_variable m_in_flight_cv;

  // Per-file dispatch backoff: after kWriteFailed, or after a dispatch threw
  // (e.g. HDF5 open failure), the file is held here until the retry window
  // expires (30 s) before being re-dispatched.
  std::unordered_map<std::string,
                     std::chrono::steady_clock::time_point> m_backoff_files;
  mutable std::mutex m_backoff_mtx;

  // Threading
  dunedaq::utilities::WorkerThread m_thread;

  std::shared_ptr<dunedaq::conffwk::Configuration> m_confdb;
  const confmodel::Application *m_application;
  std::vector<const confmodel::DaqModule *> m_modules;
  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;

  // unused to be removed
  std::string address;
  std::string data_type;
  std::string conn_type_str;

  std::string m_init_connection;
  std::vector<std::string> m_tr_connections_o;
  std::vector<std::string> m_tr_tracking_tx;
  std::string m_bk_connection_o;
  dunedaq::datafilter::Connections m_cx;

  std::chrono::milliseconds m_send_timeout_ms{100};
  std::chrono::milliseconds m_recv_timeout_ms{100};
  std::string m_trdispatcher_id;
  std::string m_tsdispatcher_id;
  std::string m_bk_info_id;
  std::string m_trdispatcher_req_rx;

  size_t m_trigger_number;
  size_t m_run_number;
  size_t run_number = 53;
  size_t fragment_size = 10 * 7200; // 10 WIBEth frames × 7200 B/frame
  size_t element_count_tpc = 4;
  size_t element_count_pds = 4;
  size_t element_count_ta = 4;
  size_t element_count_tc = 1;
  const size_t components_per_record = element_count_tpc + element_count_pds +
                                       element_count_ta + element_count_tc;

  // Always-on BK confirmation routing.  Registered in do_start() before
  // get_from_storage(); each send_tr/ts call inserts a CycleWaiter keyed by
  // trd_bk_seq and returns immediately.  The always-on BK callback sends
  // the final BK and writes WriteJSON when FRW confirms.
  struct CycleWaiter {
    std::mutex              cv_mtx;
    std::condition_variable cv;
    std::atomic<bool>       got_reply{false};
    std::vector<std::pair<std::string, std::string>> file_attrs;
    // For deferred WriteJSON in HDF5 mode
    bool        is_hdf5_mode{false};
    std::string h5_filename;
    std::string storage_pathname;
    std::string json_file;
    std::vector<std::string> file_send_list;
    // Set true only by send_ts(); selects which in-flight counter the
    // always-on BK callback releases on completion. Meaningless when
    // is_hdf5_mode is true.
    bool is_ts_waiter{false};
  };
  std::unordered_map<uint64_t, std::shared_ptr<CycleWaiter>> m_bk_waiters;
  std::mutex m_bk_waiters_mtx;
  std::atomic<uint64_t> m_bk_seq{0};
  std::shared_ptr<ReceiverConcept<dunedaq::datafilter::BookKeeping>> m_bk_always_on_rx;

  // Always-on prebuf for "next_tr"/"next_ts" requests on trdispatcher_req_rx.
  // Replaces transient add_callback/remove_callback to prevent between-cycle drops.
  std::queue<dunedaq::datafilter::Handshake> m_req_prebuf;
  std::mutex                                 m_req_prebuf_mtx;
  std::condition_variable                    m_req_prebuf_cv;
  std::shared_ptr<ReceiverConcept<dunedaq::datafilter::Handshake>> m_req_rx;

  std::atomic<bool> m_keep_running{false};
  // Two-phase barrier to solve lost-wakeup problem:
  // 1. WorkerThread sets m_worker_ready=true BEFORE waiting on the barrier CV.
  // 2. do_start() WAITS for m_worker_ready, then sets m_start_barrier, then NOTIFIES.
  // This guarantees the notification is never lost.
  std::atomic<bool> m_start_barrier{false};
  std::atomic<bool> m_worker_ready{false};
  // Set true in do_start(); cleared on first TR dispatch.  Gives ZMQ time to
  // reconnect DF's SUB socket after a TRD restart before the first kPubSub
  // publish. Storage mode only (send_tr_from_hdf5file()'s single publish
  // connection).
  std::atomic<bool> m_pub_warmup_needed{false};
  // Same idea as m_pub_warmup_needed, but generated mode's TR and TS use
  // separate kPubSub connections and so each needs its own one-shot warmup
  // wait -- sharing one flag would let whichever type loses the atomic
  // exchange race publish with no warmup at all.
  std::atomic<bool> m_tr_pub_warmup_needed{false};
  std::atomic<bool> m_ts_pub_warmup_needed{false};
  // Pointer to the WorkerThread's running_flag; valid only during do_work().
  // Used as the primary loop condition so the loop cannot exit before the
  // framework calls stop_working_thread().  m_keep_running remains the
  // wake-up signal for receive()'s prebuf wait.
  std::atomic<bool>* m_running_flag{nullptr};
  bool m_is_from_storage = false;
  bool m_generate_trigger_record = false;
  bool m_generate_time_slice = false;
  bool m_parallel_send{false};
  uint32_t m_number_generated_events{0};
  std::atomic<uint32_t> m_events_remaining{0};
  std::string m_json_file;
  std::string m_input_h5_filename;
  std::string m_output_h5_filename;
  std::string m_storage_pathname;

  // oks
  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";

  // Configuration
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  // IOManager
  std::shared_ptr<dunedaq::iomanager::SenderConcept<trigger_record_ptr_t>>
      sender;

  std::string m_session_name = "test-session";
  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER
  // READING IT m_total_amount and m_amount_since_last_get_info_call are
  // examples of variables whose values get reported to OpMon
  // (https://github.com/mozilla/opmon) each time get_info() is
  // called. "amount" represents a (discrete) value which changes as
  // TRDispatcher runs and whose value we'd like to keep track of during
  // running; obviously you'd want to replace this "in real life"

  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};
  std::atomic<uint64_t> m_tr_seq_num{0}; // counter for generated TR numbers
  std::atomic<uint64_t> m_ts_seq_num{0}; // counter for generated TS numbers

  // Generated-mode bounded in-flight window: caps the number of
  // dispatched-but-unconfirmed cycles per record type so generated dispatch
  // cannot race arbitrarily far ahead of DF/FRW. Mirrors m_in_flight_files'
  // role for storage mode, but keyed per record type with a configurable
  // window instead of hard single-flight. TR and TS are gated independently.
  std::atomic<int> m_tr_in_flight{0};
  std::atomic<int> m_ts_in_flight{0};
  std::mutex m_gen_window_mtx;
  std::condition_variable m_gen_window_cv;
  uint32_t m_generated_window{4};
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_PLUGINS_TRDISPATCHER_HPP_
