#ifndef DATAFILTER_DATAFILTERCONNECTIONS_HPP_
#define DATAFILTER_DATAFILTERCONNECTIONS_HPP_

#include <string>
#include <vector>

namespace dunedaq::datafilter {

struct Connections {
  // Control/request lanes
  std::vector<std::string> fo_ctrl; // DataFilter notify FilterOrchestrator (FO)
                                    // (df_ready), this is not used for now.
  std::vector<std::string> trdispatcher_req_rx; // TRDispatcher rx (request)
  std::vector<std::string>
      trdispatcher_req_tx; // DataFilter tx via FO -> TRDispatcher (request)

  std::vector<std::string>
      trdispatcher_req;                   // DEPRECATED, use trdispatcher_req_tx
  std::vector<std::string> trwriter_ctrl; // DataFilter -> TRReWriter   (notify)
  std::vector<std::string> tr_tracking_rx; // DataFilter rx (tracking)

  std::vector<std::string> tr_tracking_tx; // TRDispatcher tx (On TRD side)

  // TRs Data lanes
  std::vector<std::string> tr_data_rx; // TR inputs (upstream → DataFilter)
  std::vector<std::string> tr_data_tx; // TR outputs (DataFilter → downstream)

  // Bookkeeping
  std::vector<std::string> bk_outputs;
  std::vector<std::string> bk_inputs;
};

} // namespace dunedaq::datafilter
#endif
