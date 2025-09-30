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

#include "datafilter/opmon/datafilter_info.pb.h"

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

  dunedaq::conffwk::Configuration *confdb;

  try {
    confdb = new conffwk::Configuration(m_oksConfig);

  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  confdb->get<dunedaq::confmodel::Queue>(m_queues);
  confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  TLOG() << "=== Debugging Network Connections ===";
  for (const auto &conn : m_networkconnections) {

    std::string conn_id = conn->UID();
    TLOG() << "Connection: " << conn_id;

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
}

void DataFilter::do_conf(const data_t &) {
  TLOG() << get_name() << " do_conf()";
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

void DataFilter::do_start(const data_t &) { m_thread.start_working_thread(); }

void DataFilter::do_stop(const data_t &) {
  TLOG() << get_name() << " do_stop()";
  m_thread.stop_working_thread();

  TLOG() << get_name() << ": exist do_stop()";
}

void DataFilter::do_work(std::atomic<bool> &running) {

  dunedaq::datafilter::DataFilterConfig config;
  dunedaq::datafilter::RunInfo run_info;
  auto datafilter_id = std::to_string(config.my_id1);
  auto df_receiver = std::make_unique<dunedaq::datafilter::DataFilterReceiver>(
      config, run_info, datafilter_id);
  while (1) {
    TLOG() << "Request next tr";
    df_receiver->organiser.request_next_tr();
    df_receiver->receive_tr(0);
  }
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::DataFilter)
