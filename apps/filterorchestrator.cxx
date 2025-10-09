/**
 * @file filterorchestrator.cxx
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
  int result = dunedaq::datafilter::parseCommandLine(argc, argv, args,
                                                     "Filter Orchestrator");

  if (result != 2)
    return result;

  data_t filterorchestrator_cfg = {{"app_name", args.appName},
                                   {"sessionName", args.sessionName}};

  auto mgr1 = dunedaq::datafilter::make_config_mgr(
      args.appName, args.sessionName, args.oksConfig);

  TLOG() << "Creating Module instances for filterorchestrator...";
  std::shared_ptr<dunedaq::appfwk::DAQModule> filterorchestrator1 =
      make_module("FilterOrchestrator", "FilterOrchestrator_0");
  TLOG() << "Calling init on modules...";
  filterorchestrator1->init(mgr1);
  filterorchestrator1->execute_command("conf", filterorchestrator_cfg);
  filterorchestrator1->execute_command("start", filterorchestrator_cfg);
  // allow enough time for worker to enter loop at least once
  std::this_thread::sleep_for(10s);
  filterorchestrator1->execute_command("stop", filterorchestrator_cfg);

  return 0;
}
