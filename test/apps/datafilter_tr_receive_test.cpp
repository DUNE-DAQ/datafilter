/*
Datafilter : test TriggerRecord with IOManager SUB
*/

#include <execution>
#include <string>
#include <utility>
#include <vector>

#include "boost/program_options.hpp"
#include "datafilter/data_struct.hpp"
#include "detdataformats/DetID.hpp"
#include "dfmessages/TriggerRecord_serialization.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/hdf5filelayout/Nljs.hpp"
#include "hdf5libs/hdf5filelayout/Structs.hpp"
#include "hdf5libs/hdf5rawdatafile/Nljs.hpp"
#include "hdf5libs/hdf5rawdatafile/Structs.hpp"
#include "iomanager/IOManager.hpp"
#include "serialization/Serialization.hpp"

using namespace dunedaq::iomanager;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::hdf5libs;

namespace dunedaq {
namespace datafilter {

using trigger_record_ptr_t = std::unique_ptr<daqdataformats::TriggerRecord>;

struct DatafilterConfig {
    bool use_connectivity_service = false;
    int port = 5000;
    std::string server = "localhost";
    std::string info_file_base = "datafilter";
    std::string session_name = "TR testing: receiver";
    size_t num_apps = 1;
    size_t num_connections_per_group = 1;
    size_t num_groups = 1;
    size_t num_messages = 1;
    size_t message_size_kb = 1024;
    size_t num_runs = 1;
    size_t my_id = 0;
    size_t send_interval_ms = 100;
    int publish_interval = 1000;
    bool next_tr = false;

    size_t seq_number;
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t run_number;
    size_t element_id;
    size_t detector_id;
    size_t error_bits;
    size_t fragment_type;
    // dunedaq::daqdataformats::FragmentType fragment_type;

    std::string input_h5_filename =
        "/lcg/storage19/test-area/dune/trigger_records/"
        "swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";

    std::string odir = "/opt/tmp/chen";  // current directory
    std::string output_h5_filename = "h5_test";

    void configure_connsvc() {
        setenv("CONNECTION_SERVER", server.c_str(), 1);
        setenv("CONNECTION_PORT", std::to_string(port).c_str(), 1);
    }

    std::string get_connection_name(size_t app_id, size_t group_id,
                                    size_t conn_id) {
        std::stringstream ss;
        ss << "conn_A" << app_id << "_G" << group_id << "_C" << conn_id << "_";
        return ss.str();
    }
    std::string get_group_connection_name(size_t app_id, size_t group_id) {
        std::stringstream ss;
        ss << "conn_A" << app_id << "_G" << group_id << "_.*";
        return ss.str();
    }

    std::string get_connection_ip(size_t app_id, size_t group_id,
                                  size_t conn_id) {
        assert(num_apps < 253);
        assert(num_groups < 253);
        assert(num_connections_per_group < 252);

        int first_byte = conn_id + 2;    // 2-254
        int second_byte = group_id + 1;  // 1-254
        int third_byte = app_id + 1;     // 1-254

        std::string conn_addr = "tcp://127." + std::to_string(third_byte) +
                                "." + std::to_string(second_byte) + "." +
                                std::to_string(first_byte) + ":15500";

        return conn_addr;
    }

    std::string get_subscriber_init_name() {
        return get_subscriber_init_name(my_id);
    }

    std::string get_subscriber_init_name(size_t id) {
        return "conn_init_" + std::to_string(id);
    }

    void configure_iomanager() {
        setenv("DUNEDAQ_PARTITION", session_name.c_str(), 0);

        Queues_t queues;
        Connections_t connections;

        for (size_t group = 0; group < num_groups; ++group) {
            for (size_t conn = 0; conn < num_connections_per_group; ++conn) {
                auto conn_addr = get_connection_ip(my_id, group, conn);
                TLOG() << "Adding connection with id "
                       << get_connection_name(my_id, group, conn)
                       << " and address " << conn_addr;

                connections.emplace_back(Connection{
                    //            ConnectionId{ get_connection_name(my_id,
                    //            group, conn), "data_t" }, conn_addr,
                    //            ConnectionType::kPubSub });
                    ConnectionId{get_connection_name(my_id, group, conn),
                                 "TriggerRecord"},
                    conn_addr, ConnectionType::kPubSub});
            }
        }

        for (size_t sub = 0; sub < 3; ++sub) {
            auto port = 33000 + sub;
            std::string conn_addr = "tcp://127.0.0.1:" + std::to_string(port);
            TLOG() << "Adding control connection "
                   << "trwriter" + std::to_string(sub) << " with address "
                   << conn_addr;

            connections.emplace_back(Connection{
                ConnectionId{"trwriter" + std::to_string(sub), "init_t"},
                conn_addr, ConnectionType::kSendRecv});
        }

        IOManager::get()->configure(
            queues, connections, use_connectivity_service,
            std::chrono::milliseconds(publish_interval));
    }
};

struct TRRewriter {
    TRRewriter() {
        setenv("DUNEDAQ_PARTITION", "IOManager_t", 0);

        std::cout << "from TRRewriter";
    }
    ~TRRewriter() { IOManager::get()->reset(); }
    explicit TRRewriter(dunedaq::datafilter::DatafilterConfig c) : config(c) {}

    TRRewriter(TRRewriter const&) = default;
    TRRewriter(TRRewriter&&) = default;
    TRRewriter& operator=(TRRewriter const&) = default;
    TRRewriter& operator=(TRRewriter&&) = default;

    std::string session_name = "iomanager : TRRewriter test";
    bool use_connectivity_service = false;  // unsed for now
    int publish_interval = 10000;
    size_t my_id = 0;
    size_t my_gr = 0;
    size_t my_conn = 0;
    size_t num_apps = 1;
    size_t num_connections_per_group = 1;
    size_t num_groups = 1;
    size_t num_messages = 1;

    struct TRWriterInfo {
        size_t conn_id;
        size_t group_id;
        bool is_group_subscriber;
        std::unordered_map<size_t, size_t> last_sequence_received{0};
        std::atomic<size_t> msgs_received{0};
        std::atomic<size_t> msgs_with_error{0};
        std::chrono::milliseconds get_receiver_time;
        std::chrono::milliseconds add_callback_time;
        std::atomic<bool> complete{false};

        size_t messages_sent{0};
        size_t trigger_number;
        size_t trigger_timestamp;
        size_t run_number;
        size_t element_id;
        size_t detector_id;
        size_t error_bits;
        // dunedaq::daqdataformats::Fragment fragment_type;
        size_t fragment_type;
        std::string path_header;
        int n_frames;

        std::shared_ptr<
            ReceiverConcept<std::unique_ptr<dunedaq::datafilter::Data>>>
            tr_receiver;
        // std::shared_ptr<SenderConcept<dunedaq::daqdataformats::TriggerRecord>>
        // sender;
        std::unique_ptr<std::thread> receive_thread;
        // std::chrono::milliseconds get_receiver_time;

        TRWriterInfo(size_t group, size_t conn)
            : conn_id(conn), group_id(group) {}
        std::string get_connection_name(
            dunedaq::datafilter::DatafilterConfig& config) {
            if (is_group_subscriber) {
                return config.get_group_connection_name(config.my_id, group_id);
            }
            return config.get_connection_name(config.my_id, group_id, conn_id);
        }
    };
    std::string tr_writer_conn = "trwriter_conn_0";

    std::vector<std::shared_ptr<TRWriterInfo>> trwriters;
    DatafilterConfig config;

    void init() {
        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "trwriter0");
        std::atomic<std::chrono::steady_clock::time_point> last_received =
            std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - last_received.load())
                   .count() < 500) {
            dunedaq::datafilter::Handshake recv;
            recv = init_receiver->receive(std::chrono::milliseconds(100));
            TLOG() << "message received:  " << recv.msg_id;
            if (recv.msg_id == "start") break;
            std::this_thread::sleep_for(100ms);
        }

        auto init_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                "trwriter1");
        std::atomic<std::chrono::steady_clock::time_point> last_received1 =
            std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - last_received1.load())
                   .count() < 800) {
            dunedaq::datafilter::Handshake q("gotit");
            init_sender->send(std::move(q), Sender::s_block);
            std::this_thread::sleep_for(100ms);
        }
    }

    dunedaq::hdf5libs::hdf5filelayout::FileLayoutParams
    create_file_layout_params() {
        dunedaq::hdf5libs::hdf5filelayout::PathParams params_tpc;
        params_tpc.detector_group_type = "Detector_Readout";
        params_tpc.detector_group_name = "TPC";
        params_tpc.element_name_prefix = "Link";
        params_tpc.digits_for_element_number = 5;

        dunedaq::hdf5libs::hdf5filelayout::PathParamList param_list;
        param_list.push_back(params_tpc);

        dunedaq::hdf5libs::hdf5filelayout::FileLayoutParams layout_params;
        layout_params.path_param_list = param_list;
        layout_params.record_name_prefix = "TriggerRecord";
        layout_params.digits_for_record_number = 6;
        layout_params.digits_for_sequence_number = 0;
        layout_params.record_header_dataset_name = "TriggerRecordHeader";

        return layout_params;
    }

    dunedaq::hdf5libs::hdf5rawdatafile::SrcIDGeoIDMap create_srcid_geoid_map() {
        using nlohmann::json;

        dunedaq::hdf5libs::hdf5rawdatafile::SrcIDGeoIDMap map;
        json srcid_geoid_map = json::parse(R"(
        [
        {
          "source_id": 0,
          "geo_id": {
            "det_id": 3,
            "crate_id": 1,
            "slot_id": 0,
            "stream_id": 0
          }
        },
        {
          "source_id": 1,
          "geo_id": {
            "det_id": 3,
            "crate_id": 1,
            "slot_id": 0,
            "stream_id": 1
          }
        },
        {
          "source_id": 3,
          "geo_id": {
            "det_id": 3,
            "crate_id": 1,
            "slot_id": 1,
            "stream_id": 0
          }
        },
        {
          "source_id": 4,
          "geo_id": {
            "det_id": 3,
            "crate_id": 1,
            "slot_id": 1,
            "stream_id": 1
          }
        },
        {
          "source_id": 4,
          "geo_id": {
            "det_id": 2,
            "crate_id": 1,
            "slot_id": 0,
            "stream_id": 0
          }
        },
        {
          "source_id": 5,
          "geo_id": {
            "det_id": 2,
            "crate_id": 1,
            "slot_id": 0,
            "stream_id": 1
          }
        },
        {
          "source_id": 6,
          "geo_id": {
            "det_id": 2,
            "crate_id": 1,
            "slot_id": 1,
            "stream_id": 0
          }
        },
        {
          "source_id": 7,
          "geo_id": {
            "det_id": 2,
            "crate_id": 1,
            "slot_id": 1,
            "stream_id": 1
          }
        }
      ]
      )");

        return srcid_geoid_map.get<hdf5rawdatafile::SrcIDGeoIDMap>();
    }

    void receive_tr(size_t run_number1) {
        if (config.next_tr) {
            auto next_tr_sender =
                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                    "TR_tracking2");
            TLOG() << "send next_tr instruction";
            dunedaq::datafilter::Handshake q("next_tr");
            next_tr_sender->send(std::move(q), Sender::s_block);
        }

        TLOG_DEBUG(5) << "Setting up TRWriterInfo objects";
        for (size_t group = 0; group < config.num_groups; ++group) {
            // trwriters.push_back(std::make_shared<TRWriterInfo>(group));
            for (size_t conn = 0; conn < config.num_connections_per_group;
                 ++conn) {
                trwriters.push_back(
                    std::make_shared<TRWriterInfo>(group, conn));
            }
        }
        // convert file_params to json, allows for easy comp later
        dunedaq::hdf5libs::hdf5filelayout::data_t flp_json_in;
        dunedaq::hdf5libs::hdf5filelayout::to_json(flp_json_in,
                                                   create_file_layout_params());

        // create src-geo id map
        auto srcid_geoid_map = create_srcid_geoid_map();

        std::atomic<std::chrono::steady_clock::time_point> last_received =
            std::chrono::steady_clock::now();
        TLOG_DEBUG(5) << "Adding callbacks for each subscriber";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters),
            [=, &last_received](std::shared_ptr<TRWriterInfo> info) {
                auto recv_proc =
                    [=, &last_received](
                        std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>&
                            tr) {
                        auto trigger_timestamp = tr->get_fragments_ref()
                                                     .at(0)
                                                     ->get_trigger_timestamp();
                        auto trigger_number =
                            tr->get_fragments_ref().at(0)->get_trigger_number();
                        auto run_number =
                            tr->get_fragments_ref().at(0)->get_run_number();
                        int file_index = 0;

                        TLOG() << "run_number " << run_number
                               << ", trigger number: " << trigger_number;
                        info->msgs_received++;
                        last_received = std::chrono::steady_clock::now();

                        if (info->msgs_received = config.num_messages) {
                            TLOG_DEBUG(7)
                                << "Complete condition reached, sending "
                                   "init message for "
                                << info->get_connection_name(config);
                            std::string app_name = "test";
                            std::string ofile_name =
                                config.odir + "/" + config.output_h5_filename +
                                std::to_string(info->msgs_received.load()) +
                                ".hdf5";
                            TLOG()
                                << "Output trigger records to " << ofile_name;
                            // create the file
                            std::unique_ptr<HDF5RawDataFile> h5file_ptr(
                                new HDF5RawDataFile(
                                    ofile_name, run_number, file_index,
                                    app_name, flp_json_in, srcid_geoid_map,
                                    ".writing", HighFive::File::OpenOrCreate));

                            h5file_ptr->write(*tr);
                            h5file_ptr.reset();
                            info->complete = true;
                        }
                    };

                auto before_receiver = std::chrono::steady_clock::now();
                auto receiver = dunedaq::get_iom_receiver<
                    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
                    info->get_connection_name(config));
                auto after_receiver = std::chrono::steady_clock::now();
                receiver->add_callback(recv_proc);
                auto after_callback = std::chrono::steady_clock::now();
                info->get_receiver_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_receiver - before_receiver);
                info->add_callback_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_callback - after_receiver);
            });

        if (config.next_tr) {
            auto next_tr_sender =
                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                    "TR_tracking2");
            TLOG() << "send wait for next instruction";
            dunedaq::datafilter::Handshake q("wait");
            next_tr_sender->send(std::move(q), Sender::s_block);
        }

        TLOG_DEBUG(5) << "Starting wait loop for receives to complete";
        bool all_done = false;
        while (!all_done) {
            size_t recvrs_done = 0;
            for (auto& sub : trwriters) {
                if (sub->complete.load()) recvrs_done++;
            }
            TLOG_DEBUG(6) << "Done: " << recvrs_done << ", expected: "
                          << config.num_groups *
                                 config.num_connections_per_group;
            all_done = recvrs_done >=
                       config.num_groups * config.num_connections_per_group;
            if (!all_done) std::this_thread::sleep_for(1ms);
        }
        TLOG_DEBUG(5) << "Removing callbacks";
        for (auto& info : trwriters) {
            auto receiver =
                dunedaq::get_iom_receiver<dunedaq::datafilter::Data>(
                    info->get_connection_name(config));
            receiver->remove_callback();
        }

        trwriters.clear();
        TLOG_DEBUG(5) << "receive() done";
    }
};

}  // namespace datafilter

// Must be in dunedaq namespace only
DUNE_DAQ_SERIALIZABLE(datafilter::Data, "data_t");
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Handshake, "init_t");

}  // namespace dunedaq

using namespace std;

int main(int argc, char** argv) {
    dunedaq::logging::Logging::setup();
    dunedaq::datafilter::DatafilterConfig config;

    bool help_requested = false;
    namespace po = boost::program_options;
    po::options_description desc("data filter trigger records receive test.");
    desc.add_options()("odir,d",
                       po::value<std::string>(&config.odir)
                           ->default_value(config.output_h5_filename),
                       "output directory")(
        "ofilename,o",
        po::value<std::string>(&config.output_h5_filename)
            ->default_value(config.output_h5_filename),
        "output filename ")("help,h", po::bool_switch(&help_requested),
                            "For help.");

    try {
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (std::exception& ex) {
        std::cerr << "Error parsing command line " << ex.what() << std::endl;
        std::cerr << desc << std::endl;
        return 1;
    }

    if (help_requested) {
        std::cout << desc << std::endl;
        return 0;
    }

    config.configure_iomanager();

    auto trwriter = std::make_unique<dunedaq::datafilter::TRRewriter>(config);
    for (size_t run = 0; run < config.num_runs; ++run) {
        TLOG() << "TR rewriter" << config.my_id << ": "
               << "run " << run;
        if (config.num_apps > 1) trwriter->init();
        trwriter->receive_tr(run);
        TLOG() << "TR rewriter " << config.my_id << ": "
               << "run " << run << " complete.";
    }

    TLOG() << "TR rewriter" << config.my_id << ": "
           << "Cleaning up";
    trwriter.reset(nullptr);

    dunedaq::iomanager::IOManager::get()->reset();
    TLOG() << "TR rewriter " << config.my_id << ": "
           << "DONE";
}
