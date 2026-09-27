/**
 * @file DataFilter.hpp
 *
 * Developer(s) of this DAQModule have yet to replace this line with a brief
 * description of the DAQModule.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef DATAFILTER_PLUGINS_DATAFILTER_HPP_
#define DATAFILTER_PLUGINS_DATAFILTER_HPP_

#include "appfwk/DAQModule.hpp"
#include "opmonlib/TestOpMonManager.hpp"
#include "utilities/WorkerThread.hpp"

#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

#include "datafilter/bookkeeping_manager.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/core/ConnectionsBuilder.hpp"
#include "datafilter/core/DataFilterOrganiser.hpp"
#include "datafilter/core/DataFilterReceiver.hpp"
#include "datafilter/core/DataFilterTRSink.hpp"

#include "datafilter/dal/DataFilter.hpp"
#include "datafilter/opmon/datafilter_info.pb.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace dunedaq::datafilter {

class DataFilter : public dunedaq::appfwk::DAQModule,
                    public std::enable_shared_from_this<DataFilter> {
public:
  explicit DataFilter(const std::string &name);
  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;

  ~DataFilter() {
    // Guard against a destructor path that skips do_stop() (e.g. an
    // exception during startup): stop the histogram thread before any
    // member its loop touches goes away.
    if (m_hist_thread.joinable()) {
      m_hist_thread.request_stop();
      m_hist_thread.join();
    }
    if (m_bk) {
      m_bk->stop();
    }
  }

protected:
  void generate_opmon_data() override;

private:
  using data_t = nlohmann::json;
  void do_conf(const data_t &);
  void do_start(const data_t &);
  void do_stop(const data_t &);
  void do_work(std::atomic<bool> &running_flag);

  // Writes datafilter_adc_histogram.json for df_to_influx.py -- bypasses
  // opmon entirely (OpMonValue can't carry arrays); runs on its own timer
  // (m_hist_thread), not opmon's.
  void generate_influx_data();

  void print_attrs();

  dunedaq::datafilter::RunInfo m_run_info{};
  std::string m_datafilter_id;

  // Wiring
  Connections m_connections;
  std::shared_ptr<TRRewriterSink> m_sink;
  std::shared_ptr<TSRewriterSink> m_ts_sink;
  std::shared_ptr<DataFilterOrganiser> m_organiser;
  std::unique_ptr<DataFilterReceiver> m_rx;
  std::shared_ptr<dunedaq::datafilter::BookkeepingReceiver> m_bk;

  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;
  std::shared_ptr<dunedaq::conffwk::Configuration> m_confdb;

  std::vector<std::string> m_tr_connections_o;
  std::string m_bk_connection_o;

  dunedaq::utilities::WorkerThread m_thread;
  // TO datafilter DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER
  // READING IT m_total_amount and m_amount_since_last_get_info_call are
  // examples of variables whose values get reported to OpMon
  // (https://github.com/mozilla/opmon) each time get_info() is
  // called. "amount" represents a (discrete) value which changes as
  // DataFilter runs and whose value we'd like to keep track of during
  // running; obviously you'd want to replace this "in real life"

  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  std::string address;
  std::string data_type;
  std::string conn_type_str;

  std::string m_session_name = "test-session";

  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};

  // Gates generate_influx_data()'s idle-snapshot write; histogram thread only.
  bool m_hist_idle{false};

  // Drives generate_influx_data() on its own timer, independent of opmon.
  // m_hist_thread must stay the LAST member declared: members are destroyed
  // in reverse declaration order, and its loop reaches m_session_name and
  // m_hist_idle above, so it has to be stopped before either can go away.
  uint32_t m_hist_interval_s{5};
  std::jthread m_hist_thread;
};

} // namespace dunedaq::datafilter
#endif // DATAFILTER_PLUGINS_DATAFILTER_HPP_
