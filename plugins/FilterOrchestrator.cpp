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

#include "datafilter/opmon/filterorchestrator_info.pb.h"

#include <string>

namespace dunedaq::datafilter {

FilterOrchestrator::FilterOrchestrator(const std::string& name)
  : dunedaq::appfwk::DAQModule(name)
{
  register_command("conf", &FilterOrchestrator::do_conf);
}

void
FilterOrchestrator::init(std::shared_ptr<appfwk::ConfigurationManager> /* mcfg */)
{}

void
FilterOrchestrator::generate_opmon_data()
{
  opmon::FilterOrchestratorInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

void
FilterOrchestrator::do_conf(const data_t& /* do not pass an argument*/ )
{
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterOrchestrator)
