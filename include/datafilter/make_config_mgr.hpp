
namespace dunedaq {
namespace datafilter {

std::shared_ptr<dunedaq::appfwk::ConfigurationManager>
make_config_mgr(std::string app_name, std::string session_name,
                std::string oks_config) {
  if (app_name.empty() || session_name.empty() || oks_config.empty()) {
    TLOG()
        << "ERROR: app_name, session_name, and oks_config must be non-empty.";
    return nullptr;
  }

  TLOG() << "Creating ConfigurationManager..."
         << " app_name='" << app_name << "'"
         << " session_name='" << session_name << "'"
         << " oks_config='" << oks_config << "'";

  try {
    auto cfgMgr = std::make_shared<dunedaq::appfwk::ConfigurationManager>(
        oks_config, app_name, session_name);
    TLOG_DEBUG(5) << "ConfigurationManager created at: " << cfgMgr.get();
    return cfgMgr;
  } catch (const std::exception &e) {
    TLOG() << "Exception creating ConfigurationManager: " << e.what();
    return nullptr;
  }
}

} // namespace datafilter
} // namespace dunedaq
