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

void FilterOrchestrator::init_app(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {

  // const std::string session_name = "test-session";
  // std::vector<dunedaq::iomanager::ConnectionRef> m_networkconnections;

  // std::vector<dunedaq::iomanager::QueueSpec> m_queues;

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
  std::string appName = "TestApp";
  std::string connectionName = "filterorchestrator0";
  std::string session_name = "test-session";
  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";

  dunedaq::conffwk::Configuration *confdb;
  try {
    confdb = new conffwk::Configuration(m_oksConfig);

  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  // std::cout << "Attempting to get DAL session..." << std::endl;
  // auto dal_session =
  // mcfg->get_dal<dunedaq::confmodel::Session>("test-session");

  try {
    // m_application = confdb->get<confmodel::Application>(appName);
    m_application = mcfg->get_dal<confmodel::Application>(appName);
  } catch (const std::exception &e) {
    TLOG() << "Failed to get application from config: " << e.what();
    m_application = nullptr;
  }
  auto daq_app = m_application->cast<confmodel::DaqApplication>();

  if (daq_app) {
    auto modules = daq_app->get_modules();
    m_modules.assign(modules.begin(), modules.end());
  }

  std::set<std::string> connectionsAdded;

  TLOG() << "Number of modules: " << m_modules.size();
  for (auto mod : m_modules) {

    if (mod == nullptr) {
      TLOG() << "Found null module pointer!";
      continue;
    }
    TLOG() << "initialising " << mod->class_name() << " module " << mod->UID();
    auto connections = mod->get_inputs();
    auto outputs = mod->get_outputs();
    connections.insert(connections.end(), outputs.begin(), outputs.end());
    for (auto con : connections) {
      TLOG() << "Application " << con->UID();

      auto [c, inserted] = connectionsAdded.insert(con->UID());
      if (!inserted) {
        // Already handled this connection, don't add it
        continue;
      }
      auto queue = confdb->cast<confmodel::Queue>(con);
      if (queue) {
        TLOG() << "Adding queue " << queue->UID();
        m_queues.emplace_back(queue);
      }
      auto net_con = confdb->cast<confmodel::NetworkConnection>(con);
      TLOG() << "Application NetworkConnection: " << net_con->UID();
      if (net_con) {
        m_networkconnections.emplace_back(net_con);
      }
    }
  }

  dunedaq::iomanager::ConnectionInfo conn_info;
  std::vector<dunedaq::iomanager::ConnectionInfo> connection_infos;

  TLOG() << "=== Debugging Network Connections ===";
  for (const auto &conn : m_networkconnections) {

    std::string conn_id = conn->UID();
    TLOG() << "Connection: " << conn_id;

    // Get the ConfigObject using the same method as your main code
    conffwk::ConfigObject config_obj;
    try {
      confdb->get("NetworkConnection", conn_id, config_obj);

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

      // Create ConnectionInfo for IOManager
      conn_info.uid = conn_id;
      conn_info.uri = address; // This is the critical part!
      conn_info.data_type = data_type;

      // Set connection type
      if (conn_type_str == "kSendRecv") {
        conn_info.connection_type =
            dunedaq::iomanager::ConnectionType::kSendRecv;
      } else if (conn_type_str == "kPubSub") {
        conn_info.connection_type = dunedaq::iomanager::ConnectionType::kPubSub;
      }

      connection_infos.push_back(conn_info);

      TLOG() << "Successfully configured connection: " << conn_id
             << " with URI: " << address;
    } catch (const std::exception &e) {
      TLOG() << "  ERROR getting config object: " << e.what();
    }
  }

  TLOG() << "Configured " << connection_infos.size() << " network connections";

  // Process queues (convert to ConnectionInfo if needed)
  std::vector<dunedaq::iomanager::ConnectionInfo> queue_infos;
  for (const auto &queue : m_queues) {
    dunedaq::iomanager::ConnectionInfo queue_info;
    queue_info.uid = queue->UID();
    // queue_info.connection_type = dunedaq::iomanager::ConnectionType::kQueue;
    //  Queues typically don't need URIs as they're internal
    queue_infos.push_back(queue_info);
    TLOG() << "Added queue info: " << queue->UID();
  }

  // Combine all connection infos
  connection_infos.insert(connection_infos.end(), queue_infos.begin(),
                          queue_infos.end());

  TLOG() << "Configuring IOManager with " << connection_infos.size()
         << " connections";

  // Configure IOManager with the properly constructed connection infos
  dunedaq::opmonlib::TestOpMonManager opmgr;

  try {
    TLOG() << "Configure IOManager...";
    get_iomanager()->configure(session_name, m_queues, m_networkconnections,
                               nullptr, opmgr);
  } catch (const std::exception &e) {
    TLOG() << "Method 2 also failed: " << e.what();
    throw;
  }
  TLOG() << "=== End Debug ===";

  request_next_tr(0, 0);

  // Create receiver manually (example for Handshake type)
  // TLOG() << "conn_info.uid " << connection_infos[1].uid;
  // auto cb_receiver =
  // dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
  //     connection_infos[1].uid);

  // std::function<void(const dunedaq::datafilter::Handshake)> str_receiver_cb =
  //     [&](dunedaq::datafilter::Handshake msg) {
  //       TLOG() << "Received message: " << msg.msg_id;
  //     };
  // // Add callback as you already do
  // cb_receiver->add_callback(str_receiver_cb);
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
