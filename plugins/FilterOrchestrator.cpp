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

#include "datafilter/dal/FilterOrchestrator.hpp"
#include "datafilter/opmon/filterorchestrator_info.pb.h"
#include <string>

namespace dunedaq::datafilter {

FilterOrchestrator::FilterOrchestrator(const std::string &name)
    : dunedaq::appfwk::DAQModule(name) {
  register_command("conf", &FilterOrchestrator::do_conf);
}

void FilterOrchestrator::init2(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  auto iom = iomanager::IOManager::get();
  TLOG() << get_name() << ": Entering init() method";
  m_mcfg = mcfg;
  auto mdal =
      mcfg->get_dal<dunedaq::datafilter::dal::FilterOrchestrator>(get_name());
  // auto mdal = mcfg->get_dal<dunedaq::confmodel::Session>("test-session");

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  for (auto con : mdal->get_inputs()) {
    TLOG() << "Input connection data_type " << con->get_data_type() << " UID "
           << con->UID() << " datatype_to_string "
           << datatype_to_string<dunedaq::datafilter::Handshake>();
    if (con->get_data_type() ==
        datatype_to_string<dunedaq::datafilter::Handshake>()) {
      TLOG() << "Input found: " << con->get_data_type();
      m_init_connection = con->UID();
      iom->get_receiver<dunedaq::datafilter::Handshake>(m_init_connection);
    }
  }

  for (auto con : mdal->get_outputs()) {
    TLOG() << "Output connection data_type " << con->get_data_type() << " UID "
           << con->UID() << " datatype_to_string "
           << datatype_to_string<Handshake>();
    if (con->get_data_type() == datatype_to_string<Handshake>()) {
      TLOG() << "Output found: " << con->get_data_type();
      m_init_connection = con->UID();
      iom->get_sender<Handshake>(m_init_connection);
    }
  }

  m_send_timeout_ms = std::chrono::milliseconds(mdal->get_send_timeout_ms());
  m_recv_timeout_ms = std::chrono::milliseconds(mdal->get_recv_timeout_ms());

  // for test only
  m_filter_orchestrator_id = mdal->get_filter_orchestrator_id();
  TLOG() << "filter_orchestrator_id " << m_filter_orchestrator_id;

  auto cb_receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
      m_init_connection);

  std::function<void(const dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        TLOG() << "Received message: " << msg.msg_id;
      };
  // Add callback as you already do
  cb_receiver->add_callback(str_receiver_cb);
}

void FilterOrchestrator::init(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();
  std::string session_name = "test-session";

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  dunedaq::opmonlib::TestOpMonManager opmgr;
  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  try {
    TLOG() << "Configure IOManager...";
    get_iomanager()->configure(session_name, m_queues, m_networkconnections,
                               nullptr, opmgr);
  } catch (const std::exception &e) {
    TLOG() << "Failed to configure IOManager. " << e.what();
    throw;
  }

  receive(0, 0);
}

void FilterOrchestrator::send(size_t run_number, pid_t subscriber_pid) {
  std::ostringstream ss;
  auto init_receiver =
      dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("TR_tracking2");
  std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
  std::mutex tracking_mutex;

  //    for (size_t group = 0; group < config.num_groups; ++group) {
  //      for (size_t conn = 0; conn < config.num_connections_per_group;
  //      ++conn) {
  // auto info = std::make_shared<FilterOrchestratorInfo>(group, conn);
  auto info = std::make_shared<FilterOrchestratorInfo>(0, 0);
  filterorchestrators.push_back(info);
  //      }
  //    }

  TLOG_DEBUG(7) << "Getting publisher objects for each connection";
  std::for_each(std::execution::par_unseq, std::begin(filterorchestrators),
                std::end(filterorchestrators),
                [=](std::shared_ptr<FilterOrchestratorInfo> info) {
                  auto before_sender = std::chrono::steady_clock::now();
                  info->sender =
                      dunedaq::get_iom_sender<dunedaq::datafilter::Data>(
                          m_filter_orchestrator_id);
                  auto after_sender = std::chrono::steady_clock::now();
                  info->get_sender_time =
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          after_sender - before_sender);
                });

  // auto size = 1024;

  TLOG_DEBUG(7) << "Starting publish threads";
  std::for_each(
      std::execution::par_unseq, std::begin(filterorchestrators),
      std::end(filterorchestrators),
      [=, &completed_receiver_tracking,
       &tracking_mutex](std::shared_ptr<FilterOrchestratorInfo> info) {
        info->send_thread.reset(new std::thread([=,
                                                 &completed_receiver_tracking,
                                                 &tracking_mutex]() {
          bool complete_received = false;

          while (!complete_received) {
            // wait for the next TR request
            std::atomic<std::chrono::steady_clock::time_point> last_received =
                std::chrono::steady_clock::now();
            while (std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - last_received.load())
                       .count() < 500) {
              dunedaq::datafilter::Handshake recv;
              recv = init_receiver->receive(Receiver::s_block);
              TLOG() << "recv.msg_id " << recv.msg_id;
              std::this_thread::sleep_for(100ms);
              if (recv.msg_id == "wait") {
                //     if (config.next_tr) {
                auto next_tr_sender =
                    dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                        "trdispatcher2");
                TLOG() << "send wait for next instruction";
                dunedaq::datafilter::Handshake q("wait");
                next_tr_sender->send(std::move(q), Sender::s_block);
                //      }
                continue;
              } else if (recv.msg_id == "next_tr") {
                TLOG() << "Got next_tr instruction";
                //    if (config.next_tr) {
                auto next_tr_sender =
                    dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                        "trdispatcher2");
                TLOG() << "send next_tr instruction";
                dunedaq::datafilter::Handshake q("next_tr");
                next_tr_sender->send(std::move(q), Sender::s_block);
                //    }
                break;
              }
            }

            //}
            // force the while loop to end when no trigger path
            // left. complete_received = true;
          }
        }));
      });

  TLOG_DEBUG(7) << "Joining send threads";
  for (auto &sender : filterorchestrators) {
    sender->send_thread->join();
    sender->send_thread.reset(nullptr);
  }
}

void FilterOrchestrator::request_next_tr(size_t run_number,
                                         pid_t subscriber_pid) {
  bool handshake_done = false;

  std::atomic<unsigned int> sent_cnt = 0;

  auto sender_next_tr =
      dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("trdispatcher0");

  // std::chrono::milliseconds timeout(500);
  dunedaq::datafilter::Handshake sent_t1("trdispatcher0");
  // sender_next_tr->send(std::move(sent_t1), timeout);
  sender_next_tr->send(std::move(sent_t1), Sender::s_block);
}

void FilterOrchestrator::receive(size_t dataflow_run_number1,
                                 pid_t subscriber_pid) {
  bool handshake_done = false;
  std::atomic<unsigned int> received_cnt = 0;

  auto cb_receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
      "trdispatcher1");
  std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        if (msg.msg_id == "next_tr") {
          ++received_cnt;
        }
        TLOG() << "Receive instruction from filter results writer : "
               << msg.msg_id;
      };

  cb_receiver->add_callback(str_receiver_cb);
  while (!handshake_done) {
    if (received_cnt == 1)
      handshake_done = true;
  }

  cb_receiver->remove_callback();

  request_next_tr(dataflow_run_number1, subscriber_pid);
}

void FilterOrchestrator::generate_opmon_data() {
  dunedaq::datafilter::opmon::FilterOrchestratorInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

void FilterOrchestrator::do_conf(const data_t & /* do not pass an argument*/) {}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterOrchestrator)
