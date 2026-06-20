/**
 * @file FilterOrchestrator.cpp
 *
 * Implementations of FilterOrchestrator's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "FilterOrchestrator.hpp"

namespace dunedaq::datafilter {

FilterOrchestrator::FilterOrchestrator(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(std::bind(&FilterOrchestrator::do_work, this,
                         std::placeholders::_1)) {
  register_command("conf", &FilterOrchestrator::do_conf);
  register_command("start", &FilterOrchestrator::do_start);
  register_command("stop", &FilterOrchestrator::do_stop);
}

void FilterOrchestrator::init(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();

  // m_mcfg = mcfg;

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  // get FilterOrchestractor attributes.
  auto mdal =
      mcfg->get_dal<dunedaq::datafilter::dal::FilterOrchestrator>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_cx = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);
  TLOG() << "FilterOrchestractor: trdispatcher_req_tx size="
         << m_cx.trdispatcher_req_tx.size()
         << " tr_tracking_rx size=" << m_cx.tr_tracking_rx.size();
}

void FilterOrchestrator::do_conf(const data_t &) {

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

void FilterOrchestrator::do_start(const data_t &) {
  TLOG() << get_name() << ": do_start()";

  // Register always-on callback on request RX so DF's "next_tr"/"next_ts"
  // is never dropped, regardless of which app starts first.
  if (!m_cx.trdispatcher_req_rx.empty()) {
    m_req_rx = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.trdispatcher_req_rx.front());
    m_req_rx->add_callback([this](dunedaq::datafilter::Handshake msg) {
      if (msg.msg_id == "next_tr" || msg.msg_id == "next_ts") {
        std::lock_guard<std::mutex> lk(m_req_mtx);
        m_req_q.push(std::move(msg));
        m_req_cv.notify_one();
      }
    });
    TLOG() << get_name() << ": registered always-on request callback on "
           << m_cx.trdispatcher_req_rx.front();
  }

  m_thread.start_working_thread();
  TLOG() << get_name() << ": worker thread started";
}

void FilterOrchestrator::do_stop(const data_t &) {
  TLOG() << get_name() << ": do_stop()";
  m_thread.stop_working_thread();
  if (m_req_rx) {
    m_req_rx->remove_callback();
    m_req_rx.reset();
  }
  TLOG() << get_name() << ": do_stop() done";
}

void FilterOrchestrator::do_work(std::atomic<bool> &running_flag) {
  TLOG() << get_name() << ": do_work() - Starting";
  m_running_flag = &running_flag;

  while (running_flag.load()) {
    receive();
  }

  TLOG() << get_name() << ": do_work() - Exited loop";
}

void FilterOrchestrator::receive() {
  TLOG() << get_name() << ": receive() - Waiting for message from Data Filter";
  {
    std::unique_lock<std::mutex> lk(m_req_mtx);
    m_req_cv.wait(lk, [this] {
      return !m_req_q.empty() || !m_running_flag->load();
    });
    if (!m_running_flag->load()) {
      TLOG() << get_name() << ": receive() - stop requested, returning";
      return;
    }
    TLOG() << get_name() << ": received " << m_req_q.front().msg_id
           << " instruction from Data Filter";
    m_req_q.pop();
  }
  request_next_tr();
}

void FilterOrchestrator::request_next_tr() {
  if (m_cx.trdispatcher_req_tx.empty()) {
    TLOG() << "trdispatcher_req_tx endpoints is empty";
    return;
  }
  for (const auto &uid : m_cx.trdispatcher_req_tx) {
    try {
      auto s = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(uid);
      dunedaq::datafilter::Handshake send_t1("next_tr");
      // dunedaq::datafilter::Handshake send_t1("trdispatcher0");
      s->send(std::move(send_t1), std::chrono::milliseconds(500));
      TLOG() << "Sent request_next_tr TRDispatcher - Sucess to " << uid;
    } catch (const std::exception &e) {
      TLOG() << "Sent request_next_tr TRDispatcher - Failed: " << e.what();
    }
  }

  TLOG() << "Sent request_next_tr TRDispatcher - Exiting";
}

void FilterOrchestrator::generate_opmon_data() {
  dunedaq::datafilter::opmon::FilterOrchestratorInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterOrchestrator)
