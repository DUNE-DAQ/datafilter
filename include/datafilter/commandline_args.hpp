#include <boost/program_options.hpp>
#include <iostream>
#include <string>

namespace dunedaq {
namespace datafilter {

struct CommandLineArgs {
  std::string appName = "testApp";
  std::string sessionName = "test-session";
  std::string oksConfig = "oksconflibs:test/config/dfSession.data.xml";
  bool help_requested = false;
};

inline int parseCommandLine(int argc, char *argv[], CommandLineArgs &args,
                            const std::string &description = "TR Dispatcher") {
  namespace po = boost::program_options;

  po::options_description desc(description);
  desc.add_options()(
      "name,n",
      po::value<std::string>(&args.appName)->default_value(args.appName),
      "application name")("sessionName,s",
                          po::value<std::string>(&args.sessionName)
                              ->default_value(args.sessionName),
                          "session name")(
      "xml,x",
      po::value<std::string>(&args.oksConfig)->default_value(args.oksConfig),
      "oksConfig filename")("help,h", po::bool_switch(&args.help_requested),
                            "For help.");

  try {
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  } catch (std::exception &ex) {
    std::cerr << "Error parsing command line " << ex.what() << std::endl;
    std::cerr << desc << std::endl;
    return 1;
  }

  if (args.help_requested) {
    std::cout << desc << std::endl;
    return 0;
  }

  return 2; // Continue execution (success, no early exit)
}
} // namespace datafilter
} // namespace dunedaq
