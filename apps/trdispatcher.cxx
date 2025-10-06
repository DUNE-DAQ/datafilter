/**
 * @file trdispatcher.cxx
 *
 * Developer(s) of this DAQ application have yet to replace this line with a
 * brief description of the application.
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "../../appfwk/src/DAQModuleManager.hpp"
#include "appfwk/ConfigurationManager.hpp" // Needed for get_dal
#include "appfwk/DAQModule.hpp"
#include "datafilter/commandline_args.hpp"
#include "datafilter/make_config_mgr.hpp"
#include "ers/ers.hpp"

using namespace dunedaq::appfwk;
using data_t = nlohmann::json;

int main(int argc, char *argv[]) {
  dunedaq::datafilter::CommandLineArgs args;
  int result =
      dunedaq::datafilter::parseCommandLine(argc, argv, args, "TR Dispatcher");

  if (result != 2)
    return result;

  data_t trdispatcher_cfg = {{"app_name", args.appName},
                             {"sessionName", args.sessionName}};

  auto mgr1 = dunedaq::datafilter::make_config_mgr(
      args.appName, args.sessionName, args.oksConfig);

  TLOG() << "Creating Module instances for TRDispatcher...";
  std::shared_ptr<dunedaq::appfwk::DAQModule> trdispatcher1 =
      make_module("TRDispatcher", "TRDispatcher_0");
  TLOG() << "Calling init on modules...";
  trdispatcher1->init(mgr1);
  trdispatcher1->execute_command("conf", trdispatcher_cfg);
  trdispatcher1->execute_command("start", trdispatcher_cfg);
  trdispatcher1->execute_command("stop", trdispatcher_cfg);

  return 0;
}
