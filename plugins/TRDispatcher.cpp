/**
 * @file TRDispatcher.cpp
 *
 * Implementations of TRDispatcher's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TRDispatcher.hpp"

#include "datafilter/opmon/trdispatcher_info.pb.h"

#include <string>

namespace dunedaq::datafilter {

TRDispatcher::TRDispatcher(const std::string& name)
  : dunedaq::appfwk::DAQModule(name)
{
  register_command("conf", &TRDispatcher::do_conf);
}

void
TRDispatcher::init(std::shared_ptr<appfwk::ConfigurationManager> /* mcfg */)
{}

void
TRDispatcher::generate_opmon_data()
{
  opmon::TRDispatcherInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

void
TRDispatcher::do_conf(const data_t& /* do not pass an argument*/ )
{
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::TRDispatcher)
