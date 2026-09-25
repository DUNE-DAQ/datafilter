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
#include "datafilter/core/Connections.hpp"
#include "datafilter/core/ConnectionsBuilder.hpp"
#include "datafilter/dal/FilterOrchestrator.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "datafilter/opmon/filterorchestrator_info.pb.h"
#include "opmonlib/TestOpMonManager.hpp"
#include "utilities/WorkerThread.hpp"

#include <atomic>
#include <condition_variable>
#include <execution>
#include <limits>
#include <mutex>
#include <queue>
#include <string>

using data_t = nlohmann::json;
using namespace dunedaq::iomanager;

namespace dunedaq::datafilter {

class FilterOrchestrator : public dunedaq::appfwk::DAQModule {
public:
  explicit FilterOrchestrator(const std::string &name);

  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;
  void send();
  void request_next_tr();
  void relay_request(const std::string &msg_id);
  void receive();

  FilterOrchestrator(const FilterOrchestrator &) = delete;
  FilterOrchestrator &operator=(const FilterOrchestrator &) = delete;
  FilterOrchestrator(FilterOrchestrator &&) = delete;
  FilterOrchestrator &operator=(FilterOrchestrator &&) = delete;

  ~FilterOrchestrator() = default;

protected:
  void generate_opmon_data() override;

private:
  // Commands FilterOrchestrator can receive
  void do_conf(const data_t &);
  void do_start(const data_t &);
  void do_stop(const data_t &);
  void do_work(std::atomic<bool> &);

  // Threading
  dunedaq::utilities::WorkerThread m_thread;

  std::shared_ptr<dunedaq::conffwk::Configuration> m_confdb;
  const confmodel::Application *m_application;
  std::vector<const confmodel::DaqModule *> m_modules;
  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;
  Connections m_cx;

  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";
  std::string m_session_name = "test-session";

  // Configuration
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  std::atomic<bool> *m_running_flag{nullptr};
  std::string m_init_connection;
  std::string m_filter_orchestrator_id;
  std::chrono::milliseconds m_send_timeout_ms{100};
  std::chrono::milliseconds m_recv_timeout_ms{100};
  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};

  // Always-on request prebuf (same pattern as TRDispatcher)
  std::queue<dunedaq::datafilter::Handshake> m_req_q;
  std::mutex m_req_mtx;
  std::condition_variable m_req_cv;
  std::shared_ptr<ReceiverConcept<dunedaq::datafilter::Handshake>> m_req_rx;

  // Own stop flag rather than reusing WorkerThread's running_flag: that flag is
  // cleared by stop_working_thread(), which also blocks in join() -- so there
  // is no point at which do_stop() could notify m_req_cv and still have the
  // waiter re-evaluate. This flag is set and notified before the join.
  std::atomic<bool> m_stopping{false};
};

} // namespace dunedaq::datafilter

#endif // DFBACKEND_PLUGINS_FILTERORCHESTRATOR_HPP_
