/**
 * @file DataFilter.cpp
 *
 * Implementations of DataFilter's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "DataFilter.hpp"

#include <fstream>

namespace dunedaq::datafilter {

DataFilter::DataFilter(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(std::bind(&DataFilter::do_work, this, std::placeholders::_1)) {
  register_command("conf", &DataFilter::do_conf);
  register_command("start", &DataFilter::do_start);
  register_command("stop", &DataFilter::do_stop);
}

void DataFilter::init(std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();

  m_mcfg = mcfg;

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  print_attrs();
}

void DataFilter::print_attrs() {

  TLOG() << "=== Debugging Network Connections ===";
  for (const auto &conn : m_networkconnections) {

    std::string conn_id = conn->UID();
    TLOG() << "Connection: " << conn_id;

    conffwk::ConfigObject config_obj;
    try {
      m_confdb->get("NetworkConnection", conn_id, config_obj);

      // Check what attributes are available
      TLOG() << "  Available attributes:";

      try {
        config_obj.get("address", address);
        TLOG() << "    address: '" << address << "'";
      } catch (...) {
        TLOG() << "    address: NOT FOUND";
      }

      try {
        config_obj.get("data_type", data_type);
        TLOG() << "Data type: " << data_type;
      } catch (...) {
        TLOG() << "Using default data type";
        data_type = "init_t";
      }

      try {
        config_obj.get("connection_type", conn_type_str);
        TLOG() << "Connection type: " << conn_type_str;
      } catch (...) {
        TLOG() << "Using default connection type";
        conn_type_str = "kSendRecv";
      }
    } catch (const std::exception &e) {
      TLOG() << "  ERROR getting config object: " << e.what();
    }
  }
}

void DataFilter::generate_opmon_data() {

  opmon::DataFilterInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));

  generate_influx_data();
}

void DataFilter::generate_influx_data() {
  // Accept/reject ADC histograms bypass opmon entirely: OpMonValue only
  // supports scalars, so a repeated field here would be silently dropped
  // (see datafilter_info.proto). Written directly to our own file instead,
  // on the same cadence as the opmon publish above. Bare relative filename
  // -> lands in the process's cwd, i.e. dfcontrol.sh's "running directory"
  // (INVOKE_DIR), same convention as the bookkeeping_*.json files.
  if (m_rx && m_rx->m_alg.enable_histogram) {
    auto [accepted, rejected] = m_rx->m_alg.take_histograms();
    nlohmann::json j;
    j["session"] = m_session_name;
    j["app"] = get_name();
    j["accepted_adc_histogram"] = accepted;
    j["rejected_adc_histogram"] = rejected;
    std::ofstream f("datafilter_adc_histogram.json");
    if (f.is_open()) {
      f << j.dump(2);
    } else {
      TLOG() << "generate_influx_data: failed to open "
                "datafilter_adc_histogram.json for writing";
    }
  }
}

void DataFilter::do_conf(const data_t &cfg) {
  TLOG() << get_name() << " do_conf()";

  // Real opmon manager -- this app's main() bypasses appfwk::Application, so
  // there is no framework-provided OpMonManager/register_node/start_monitoring
  // wiring anywhere else. Build it here so generate_opmon_data() actually
  // fires. m_mcfg->session() works without initialize() (DataFilter_0 isn't
  // an Application-typed OKS object); get_dal<T>(name) is a generic by-name
  // fetch, so the OpMonConf lookup below doesn't need one either.
  const std::string opmon_uri =
      m_mcfg->session()->get_opmon_uri()->get_URI(get_name());
  m_opmgr = std::make_shared<dunedaq::opmonlib::OpMonManager>(
      m_session_name, get_name(), opmon_uri);
  auto opmon_conf = m_mcfg->get_dal<dunedaq::confmodel::OpMonConf>(
      "datafilter-opmon-conf");
  m_opmgr->set_opmon_conf(opmon_conf);
  m_opmgr->register_node(get_name(), shared_from_this());

  try {
    TLOG() << "Configure IOManager...";
    get_iomanager()->configure(m_session_name, m_queues, m_networkconnections,
                               nullptr, *m_opmgr);
  } catch (const std::exception &e) {
    TLOG() << "Failed to configure IOManager. " << e.what();
    throw;
  }

  // get DataFilter attributes.
  auto mdal = m_mcfg->get_dal<dunedaq::datafilter::dal::DataFilter>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_connections = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);

  if (!m_connections.trdispatcher_req_tx.empty())
    TLOG() << "TRDispatcher request tx.front(): "
           << m_connections.trdispatcher_req_tx.front();
  else
    TLOG() << "TRDispatcher request tx is empty.";

  m_datafilter_id = mdal->get_datafilter_id();
  const uint16_t adc_threshold =
      static_cast<uint16_t>(mdal->get_adc_threshold());
  const bool enable_df_influx = mdal->get_enable_df_influx();
  const bool enable_frame_filter = mdal->get_enable_frame_filter();
  TLOG() << "DataFilter: adc_threshold=" << adc_threshold
         << " enable_df_influx=" << enable_df_influx
         << " enable_frame_filter=" << enable_frame_filter;

  // bookkeeping first
  m_bk = std::make_shared<dunedaq::datafilter::BookkeepingReceiver>(
      m_run_info, m_datafilter_id,
      m_connections.bk_inputs.empty() ? "" : m_connections.bk_inputs.front(),
      m_connections.bk_outputs.empty()
          ? ""
          : m_connections.bk_outputs.front(), // bookkeeping1 -> FRW
      m_session_name,
      m_connections.bk_outputs.size() > 1 ? m_connections.bk_outputs.at(1)
                                          : ""); // bookkeeping2 -> TRD

  m_bk->start();
  // Wire sink -> organiser -> receiver
  m_sink = std::make_shared<dunedaq::datafilter::TRRewriterSink>(
      m_connections, dunedaq::datafilter::SendPolicy::First);
  m_sink->bind_bookkeeping(m_bk);

  m_organiser = std::make_shared<DataFilterOrganiser>(m_connections, m_sink);

  // Wire TS sink if TS outputs are configured
  if (!m_connections.ts_data_tx.empty()) {
    m_ts_sink =
        std::make_shared<dunedaq::datafilter::TSRewriterSink>(m_connections);
    m_ts_sink->bind_bookkeeping(m_bk);
    m_organiser->ts_writer = m_ts_sink;
    TLOG() << "TS pipeline enabled: ts_data_tx="
           << m_connections.ts_data_tx.size();
  }
  m_rx =
      std::make_unique<DataFilterReceiver>(m_connections, m_organiser, *m_bk,
                                           /* attach_tracking_inputs */ true);
  m_rx->m_alg.adc_threshold = adc_threshold;
  m_rx->m_alg.enable_histogram = enable_df_influx;
  m_rx->m_alg.frame_level_filter = enable_frame_filter;
  m_rx->prefetch_window = mdal->get_prefetch_window();

  TLOG() << "DF Connections summary: "
         << "TR data inputs=" << m_rx->cx.tr_data_rx.size()
         << " tracking inputs=" << m_rx->cx.tr_tracking_rx.size()
         << " dispatcher req=" << m_rx->cx.trdispatcher_req_tx.size();
  for (auto &uid : m_rx->cx.tr_data_rx) {
    TLOG() << "TR data uid: " << uid;
  }

  // Pre-warm PULL sockets so they exist before TRD/FRW send tracking/BK
  // messages. IOManager creates sockets lazily; without this, cold-start sends
  // are dropped.
  if (!m_connections.tr_tracking_rx.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_connections.tr_tracking_rx.front());
    TLOG() << "DF: pre-warmed tr_tracking_rx PULL on "
           << m_connections.tr_tracking_rx.front();
  }
  if (!m_connections.bk_inputs.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
        m_connections.bk_inputs.front());
    TLOG() << "DF: pre-warmed bk_inputs PULL on "
           << m_connections.bk_inputs.front();
  }

  // Pre-create PUB/PUSH sender sockets in do_conf() so ZMQ connections
  // are established before any data flows in do_start().
  // This mitigates the ZMQ slow-joiner issue on cold start.
  if (!m_connections.tr_data_tx.empty() &&
      !m_connections.trwriter_ctrl.empty()) {
    const std::string tr_data_uid = m_connections.tr_data_tx.at(0);
    const std::string ctrl_uid = m_connections.trwriter_ctrl.at(0);
    m_sink->init(tr_data_uid, ctrl_uid);
    TLOG() << "DF: pre-created TR sender on " << tr_data_uid;
  }
  if (m_ts_sink && !m_connections.ts_data_tx.empty()) {
    const std::string ts_data_uid = m_connections.ts_data_tx.at(0);
    const std::string ts_ctrl_uid =
        m_connections.tswriter_ctrl.empty()
            ? (m_connections.trwriter_ctrl.empty()
                   ? ""
                   : m_connections.trwriter_ctrl.at(0))
            : m_connections.tswriter_ctrl.at(0);
    m_ts_sink->init(ts_data_uid, ts_ctrl_uid);
    TLOG() << "DF: pre-created TS sender on " << ts_data_uid;
  }
}

void DataFilter::do_start(const data_t & /*cfg*/) {
  // Register callbacks on all TR inputs; forward-on-arrival
  // pull mode
  m_rx->queue_only = false;
  m_rx->pull_mode = true;
  // prefetch_window is set from OKS config in do_conf().

  m_rx->start();

  if (m_opmgr) {
    m_opmgr->start_monitoring();
  }
}

void DataFilter::do_stop(const data_t & /*cfg*/) {

  TLOG() << get_name() << " do_stop()";

  // Wait for all bookkeeping entries (TRD initial, FRW completion, TRD final)
  // before letting the framework tear down IOM connections.  Without this
  // the receiver threads die before FRW and TRD have sent their final BK.
  if (m_bk) {
    TLOG() << "do_stop(): waiting for bookkeeping completion...";
    m_bk->stop();
    TLOG() << "do_stop(): bookkeeping complete.";
  }
}

void DataFilter::do_work(std::atomic<bool> &running_flag) {

  std::mutex work_mutex;
  std::condition_variable work_cv;

  // dunedaq::datafilter::DataFilterConfig config;
  // dunedaq::datafilter::RunInfo run_info;
  // auto datafilter_id = std::to_string(config.my_id1);
  // auto df_receiver =
  // std::make_unique<dunedaq::datafilter::DataFilterReceiver>(
  //     config, run_info, datafilter_id);
  // while (running_flag.load()) {
  //   TLOG() << "Request next tr";
  //   df_receiver->organiser.request_next_tr();
  //   df_receiver->receive_tr();

  //   std::unique_lock<std::mutex> lock(work_mutex);
  //   work_cv.wait_for(lock, std::chrono::seconds(1), [&]() {
  //     return !running_flag.load(); // check for new work availability
  //   });
  // }
}
} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::DataFilter)
