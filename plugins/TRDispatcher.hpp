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
#include <execution>
#include <functional>
#include <limits>
#include <string>

#include "appfwk/DAQModule.hpp"
#include "confmodel/DaqApplication.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeaderData.hpp"
#include "datafilter/HDF5FromStorage.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "datafilter/node_info.hpp"
#include "dfmessages/TriggerRecord_serialization.hpp"
#include "opmonlib/TestOpMonManager.hpp"
#include "utilities/WorkerThread.hpp"

#include "conffwk/ConfigObject.hpp"
#include "conffwk/ConfigObjectImpl.hpp"

#include "datafilter/dal/TRDispatcher.hpp"
#include "datafilter/opmon/trdispatcher_info.pb.h"
#include "detdataformats/DetID.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"

using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

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
    // dunedaq::daqdataformats::Fragment fragment_type;
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
  void init2(std::shared_ptr<appfwk::ConfigurationManager>);
  void receive(size_t dataflow_run_number1, pid_t subscriber, bool is_hdf5file);

  void send_tr_from_hdf5file(size_t dataflow_run_number1, pid_t subscriber);
  void send_tr(size_t dataflow_run_number1, pid_t subscriber);
  trigger_record_ptr_t create_trigger_record(uint64_t trig_num);
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
  void do_work(std::atomic<bool> &running);
  void do_h5file_work(std::atomic<bool> &running);

  // Threading
  dunedaq::utilities::WorkerThread m_thread;
  dunedaq::utilities::WorkerThread m_h5file_thread;

  const confmodel::Application *m_application;
  std::vector<const confmodel::DaqModule *> m_modules;
  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;
  std::string address;
  std::string data_type;
  std::string conn_type_str;

  std::string m_init_connection;
  std::string m_trigger_record_connection;
  std::chrono::milliseconds m_send_timeout_ms{100};
  std::chrono::milliseconds m_recv_timeout_ms{100};
  std::string m_trdispatcher_id;
  std::string m_bk_info_id;
  size_t m_trigger_number;
  size_t m_run_number;
  size_t run_number = 53;
  size_t fragment_size = 100;
  size_t element_count_tpc = 4;
  size_t element_count_pds = 4;
  size_t element_count_ta = 4;
  size_t element_count_tc = 1;
  const size_t components_per_record = element_count_tpc + element_count_pds +
                                       element_count_ta + element_count_tc;

  bool m_is_from_storage = false;
  std::string json_file = "hdf5_files_list.json";
  std::string m_input_h5_filename =
      "/lcg/storage19/test-area/dune/trigger_records/"
      "swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";
  std::string m_output_h5_filename = "/opt/tmp/chen/h5_test.hdf5";
  std::string m_storage_pathname =
      "/lcg/storage19/test-area/dune-v4-spack-integration2/sourcecode/"
      "daqconf/config/";

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
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_PLUGINS_TRDISPATCHER_HPP_
