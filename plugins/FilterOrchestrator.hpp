/**
 * @file FilterOrchestrator.hpp
 *
 * Developer(s) of this DAQModule have yet to replace this line with a brief
 * description of the DAQModule.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef DFBACKEND_PLUGINS_FILTERORCHESTRATOR_HPP_
#define DFBACKEND_PLUGINS_FILTERORCHESTRATOR_HPP_

#include "appfwk/DAQModule.hpp"
#include "confmodel/DaqApplication.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "opmonlib/TestOpMonManager.hpp"
#include "utilities/WorkerThread.hpp"

#include <atomic>
#include <execution>
#include <limits>
#include <string>

using data_t = nlohmann::json;
using namespace dunedaq::iomanager;

namespace dunedaq::datafilter {

class FilterOrchestrator : public dunedaq::appfwk::DAQModule {
public:
  struct FilterOrchestratorInfo {
    size_t conn_id;
    size_t group_id;
    size_t messages_sent{0};
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t run_number;
    size_t element_id;
    size_t detector_id;
    size_t error_bits;
    // dunedaq::daqdataformats::Fragment fragment_type;
    size_t fragment_type;
    std::string path_header;
    int n_frames;

    std::shared_ptr<
        dunedaq::iomanager::SenderConcept<dunedaq::datafilter::Data>>
        sender;
    std::unique_ptr<std::thread> send_thread;
    std::chrono::milliseconds get_sender_time;

    FilterOrchestratorInfo(size_t group, size_t conn)
        : conn_id(conn), group_id(group) {}
  };

  explicit FilterOrchestrator(const std::string &name);

  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;
  void init2(std::shared_ptr<appfwk::ConfigurationManager>);
  void send(size_t run_number, pid_t subscriber_pid);
  void request_next_tr(size_t run_number, pid_t subscriber_pid);
  void receive(size_t dataflow_run_number1, pid_t subscriber_pid);

  std::vector<std::shared_ptr<FilterOrchestratorInfo>> filterorchestrators;
  FilterOrchestrator(const FilterOrchestrator &) = delete;
  FilterOrchestrator &operator=(const FilterOrchestrator &) = delete;
  FilterOrchestrator(FilterOrchestrator &&) = delete;
  FilterOrchestrator &operator=(FilterOrchestrator &&) = delete;

  ~FilterOrchestrator() = default;

protected:
  void generate_opmon_data() override;

private:
  // Commands FilterOrchestrator can receive

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER READING
  // IT For any run control command it is possible for a DAQModule to register
  // an action that will be executed upon reception of the command. do_conf is a
  // very common example of this; in FilterOrchestrator.cpp you would implement
  // do_conf so that members of FilterOrchestrator get assigned values from a
  // configuration passed as an argument and originating from the CCM system.

  void do_conf(const data_t &);

  const confmodel::Application *m_application;
  std::vector<const confmodel::DaqModule *> m_modules;
  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;
  std::string address;
  std::string data_type;
  std::string conn_type_str;

  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";

  // Configuration
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER READING
  // IT m_total_amount and m_amount_since_last_get_info_call are examples of
  // variables whose values get reported to OpMon
  // (https://github.com/mozilla/opmon) each time get_info() is
  // called. "amount" represents a (discrete) value which changes as
  // FilterOrchestrator runs and whose value we'd like to keep track of during
  // running; obviously you'd want to replace this "in real life"

  std::string m_init_connection;
  std::string m_filter_orchestrator_id;
  std::chrono::milliseconds m_send_timeout_ms{100};
  std::chrono::milliseconds m_recv_timeout_ms{100};
  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};
};

} // namespace dunedaq::datafilter

#endif // DFBACKEND_PLUGINS_FILTERORCHESTRATOR_HPP_
