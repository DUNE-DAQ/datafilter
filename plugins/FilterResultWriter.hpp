/**
 * @file FilterResultWriter.hpp
 *
 * Developer(s) of this DAQModule have yet to replace this line with a brief
 * description of the DAQModule.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef DFBACKEND_PLUGINS_FILTERRESULTWRITER_HPP_
#define DFBACKEND_PLUGINS_FILTERRESULTWRITER_HPP_

#include "appfwk/DAQModule.hpp"

#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeaderData.hpp"
#include "datafilter/dal/FilterResultWriter.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "datafilter/opmon/filterresultwriter_info.pb.h"
#include "dfmessages/TriggerRecord_serialization.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/test/HDF5TestUtils.hpp"
#include "opmonlib/TestOpMonManager.hpp"
// #include "serialization/Serialization.hpp"
#include "utilities/WorkerThread.hpp"

#include <atomic>
#include <execution>
#include <limits>
#include <string>

using namespace dunedaq::iomanager;
using namespace dunedaq::hdf5libs;
using dataobj_t = nlohmann::json;
using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

namespace dunedaq::datafilter {

class FilterResultWriter : public dunedaq::appfwk::DAQModule {
public:
  struct FilterResultWriterInfo {
    size_t conn_id;
    size_t group_id;
    size_t messages_sent{0};
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t run_number;
    size_t element_id;
    size_t detector_id;
    size_t error_bits;
    size_t fragment_type;
    std::string path_header;
    int n_frames;

    std::shared_ptr<SenderConcept<dunedaq::datafilter::Data>> sender;
    std::unique_ptr<std::thread> send_thread;
    std::chrono::milliseconds get_sender_time;

    FilterResultWriterInfo(size_t group, size_t conn)
        : conn_id(conn), group_id(group) {}
  };

  struct SubscriberInfo {
    size_t group_id;
    size_t conn_id;
    bool is_group_subscriber;
    std::unordered_map<size_t, size_t> last_sequence_received{0};
    std::atomic<size_t> msgs_received{0};
    std::atomic<size_t> msgs_with_error{0};
    std::chrono::milliseconds get_receiver_time;
    std::chrono::milliseconds add_callback_time;
    std::atomic<bool> complete{false};

    SubscriberInfo(size_t group, size_t conn)
        : group_id(group), conn_id(conn), is_group_subscriber(false) {}
    SubscriberInfo(size_t group)
        : group_id(group), conn_id(0), is_group_subscriber(true) {}

    // std::string get_connection_name(FilterResultWriterConfig &config) {
    //   if (is_group_subscriber) {
    //     return config.get_group_connection_name(config.my_id, group_id);
    //   }
    //   return config.get_connection_name(config.my_id, group_id, conn_id);
    // }
  };

  explicit FilterResultWriter(const std::string &name);

  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;

  void set_file_index(uint32_t file_index) {
    m_file_index.store(file_index, std::memory_order_release);
  }
  size_t get_file_index() const {
    return m_file_index.load(std::memory_order_acquire);
  }
  std::string generate_hdf5file_pathname(std::string file_pathname_prefix,
                                         int run_number, int file_index,
                                         int trigger_number);
  void receive_tr(size_t run_number1);
  void receive_tr_single_connection(size_t run_number1);
  void send_next_tr(size_t run_number, pid_t subscriber_pid);
  void receive_attrs_test();
  void start_receive_attrs_test_thread();
  void stop_receive_attrs_test_thread();

  std::vector<std::shared_ptr<SubscriberInfo>> subscribers;
  FilterResultWriter(const FilterResultWriter &) = delete;
  FilterResultWriter &operator=(const FilterResultWriter &) = delete;
  FilterResultWriter(FilterResultWriter &&) = delete;
  FilterResultWriter &operator=(FilterResultWriter &&) = delete;

  ~FilterResultWriter() = default;

protected:
  void generate_opmon_data() override;

private:
  // Commands FilterResultWriter can receive

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER READING
  // IT For any run control command it is possible for a DAQModule to register
  // an action that will be executed upon reception of the command. do_conf is a
  // very common example of this; in FilterResultWriter.cpp you would implement
  // do_conf so that members of FilterResultWriter get assigned values from a
  // configuration passed as an argument and originating from the CCM system.

  void do_conf(const data_t &);
  void do_start(const data_t &);
  void do_stop(const data_t &);
  void do_work(std::atomic<bool> &running);
  // void attrs_thread(std::atomic<bool> &running);
  void receive_attrs(std::atomic<bool> &running);

  // Threading
  dunedaq::utilities::WorkerThread m_thread;
  dunedaq::utilities::WorkerThread m_bk_thread;

  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER READING
  // IT m_total_amount and m_amount_since_last_get_info_call are examples of
  // variables whose values get reported to OpMon
  // (https://github.com/mozilla/opmon) each time get_info() is
  // called. "amount" represents a (discrete) value which changes as
  // FilterResultWriter runs and whose value we'd like to keep track of during
  // running; obviously you'd want to replace this "in real life"

  // Configuration
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";
  std::string m_session_name = "test-session";
  size_t m_trigger_timestamp;
  size_t m_trigger_number;
  size_t m_run_number;
  std::atomic<size_t> m_num_messages{1};
  std::string m_info_file_base = "FilterResultWriter";
  std::string m_odir = "/opt/tmp/chen";
  std::string m_output_h5_filename = "h5_test";
  // std::string m_session_name = "FilterResultWriter test run";
  std::string m_ofile_pathname{};

  std::string m_init_connection;
  std::atomic<int> m_num_groups{1};
  std::atomic<int> m_num_connections_per_group{5};
  std::atomic<size_t> m_file_index{0};

  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};

  // for testing only, not used and to be removed.
  std::thread m_attrs_test_thread;
  std::atomic<bool> m_attrs_test_running{false};
  std::mutex m_attrs_test_mtx;
  std::condition_variable m_attrs_test_cv;
};

} // namespace dunedaq::datafilter

#endif // DFBACKEND_PLUGINS_FILTERRESULTWRITER_HPP_
