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

#include <string>

namespace dunedaq::datafilter {

DataFilter::DataFilter(const std::string& name)
  : dunedaq::appfwk::DAQModule(name)
{
  register_command("conf", &DataFilter::do_conf);
}

void
DataFilter::init(std::shared_ptr<appfwk::ConfigurationManager> /* mcfg */)
{}

void
DataFilter::generate_opmon_data()
{
  opmon::DataFilterInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

void
DataFilter::do_conf(const data_t& /* do not pass an argument*/ )
{
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::DataFilter)
