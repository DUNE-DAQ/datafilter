/*
Datafilter : TriggerRecord send test with IOManager PUB
*/

#include <stdlib.h>

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
using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::detdataformats;

namespace dunedaq {
namespace datafilter {

using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

struct DatafilterConfig {
    bool use_connectivity_service = false;  // unsed for now
    int port = 5000;
    std::string server = "localhost";

    std::string info_file_base = "datafilter";
    std::string session_name = "TR testing: Sender";
    size_t num_apps = 1;
    size_t num_connections_per_group = 1;
    size_t num_groups = 1;
    size_t num_messages = 1;
    size_t message_size_kb = 1024;
    size_t num_runs = 1;
    size_t my_id = 0;
    size_t send_interval_ms = 100;
    int publish_interval = 10000;
    bool next_tr = false;

    size_t seq_number;
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t run_number;
    size_t element_id;
    size_t detector_id;
    size_t error_bits;
    // size_t fragment_type;
    // dunedaq::daqdataformats::Fragment fragment_type;

    std::string input_h5_filename =
        "/lcg/storage19/test-area/dune/trigger_records/"
        "swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";
    std::string output_h5_filename = "/opt/tmp/chen/h5_test.hdf5";
    size_t write_fragment_type = 1;  // 0 -> TPC; 1 -> Trigger
    // std::string hw_map_file_name="testwriter_hardwaremap.txt";

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
        int third_byte = app_id + 1;     // 1 - 254

        std::string conn_addr = "tcp://127." + std::to_string(third_byte) +
                                "." + std::to_string(second_byte) + "." +
                                std::to_string(first_byte) + ":15500";

        return conn_addr;
    }

    std::string get_pub_init_name() { return get_pub_init_name(my_id); }
    std::string get_pub_init_name(size_t id) {
        return "conn_init_" + std::to_string(id);
    }
    // std::string get_publisher_init_name() { return "conn_init_.*"; }

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
                    ConnectionId{get_connection_name(my_id, group, conn),
                                 "TriggerRecord"},
                    conn_addr, ConnectionType::kPubSub});
            }
        }

        //    auto conn_addr = get_connection_ip(my_id, my_gr, my_conn);
        //    connections.emplace_back(Connection{
        //            ConnectionId{ get_connection_name(my_id, my_gr, my_conn),
        //            "data_t" }, conn_addr, ConnectionType::kPubSub });

        //      for (size_t sub = 0; sub < num_apps; ++sub) {
        for (size_t sub = 0; sub < 3; ++sub) {
            auto port = 13000 + sub;
            std::string conn_addr = "tcp://127.0.0.1:" + std::to_string(port);
            TLOG() << "Adding control connection "
                   << "TR_tracking" + std::to_string(sub) << " with address "
                   << conn_addr;

            connections.emplace_back(
                // Connection{ ConnectionId{ "TR_tracking"+std::to_string(sub),
                // "init_t" }, conn_addr, ConnectionType::kPubSub });
                Connection{
                    ConnectionId{"TR_tracking" + std::to_string(sub), "init_t"},
                    conn_addr, ConnectionType::kSendRecv});
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
    std::string input_h5_filename =
        "/lcg/storage19/test-area/dune/trigger_records/"
        "swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";
    size_t fragment_size = 100;
    size_t element_count_tpc = 4;
    size_t element_count_pds = 4;
    size_t element_count_ta = 4;
    size_t element_count_tc = 1;
    const size_t components_per_record = element_count_tpc + element_count_pds +
                                         element_count_ta + element_count_tc;
    int run_number = 53;
    int file_index = 0;
    const std::string application_name = "HDF5WriteReadTriggerRecord_test";

    std::string get_connection_name(size_t app_id, size_t group_id,
                                    size_t conn_id) {
        std::stringstream ss;
        ss << "conn_A" << app_id << "_G" << group_id << "_C" << conn_id << "_";
        return ss.str();
    }
    std::string get_connection_ip(size_t app_id, size_t group_id,
                                  size_t conn_id) {
        assert(num_apps < 253);
        assert(num_groups < 253);
        assert(num_connections_per_group < 252);

        int first_byte = conn_id + 2;    // 2-254
        int second_byte = group_id + 1;  // 1-254
        int third_byte = app_id + 1;     // 1 - 254

        std::string conn_addr = "tcp://127." + std::to_string(third_byte) +
                                "." + std::to_string(second_byte) + "." +
                                std::to_string(first_byte) + ":15500";

        return conn_addr;
    }
    struct TRWriterInfo {
        size_t conn_id;
        size_t group_id;
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

        std::shared_ptr<SenderConcept<
            std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>>
            sender;
        // std::shared_ptr<SenderConcept<dunedaq::datafilter::Data>> sender;
        std::unique_ptr<std::thread> send_thread;
        std::chrono::milliseconds get_sender_time;

        TRWriterInfo(size_t group, size_t conn)
            : conn_id(conn), group_id(group) {}
    };
    std::string tr_writer_conn = "trwriter_conn_0";

    std::vector<std::shared_ptr<TRWriterInfo>> trwriters;
    dunedaq::datafilter::DatafilterConfig config;

    uint16_t data3[200000000];
    uint32_t nchannels = 64;
    uint32_t nsamples = 64;

    std::string path_header1;

    void init() {
        auto init_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                "trwriter0");
        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "trwriter1");
        std::atomic<std::chrono::steady_clock::time_point> last_received =
            std::chrono::steady_clock::now();

        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - last_received.load())
                   .count() < 50000) {
            dunedaq::datafilter::Handshake q("start");
            init_sender->send(std::move(q), Sender::s_block);
            dunedaq::datafilter::Handshake recv;
            recv = init_receiver->receive(std::chrono::milliseconds(100));
            std::this_thread::sleep_for(100ms);
            if (recv.msg_id == "gotit") TLOG() << "Receiver got it";
            break;
        }
    }

    hdf5filelayout::FileLayoutParams create_file_layout_params() {
        // set TPC layout params
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

    hdf5rawdatafile::SrcIDGeoIDMap create_srcid_geoid_map() {
        using nlohmann::json;

        hdf5rawdatafile::SrcIDGeoIDMap map;
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

    dunedaq::datafilter::trigger_record_ptr_t create_trigger_record(
        uint64_t trig_num) {
        // setup our dummy_data
        std::vector<char> dummy_vector(fragment_size);
        char* dummy_data = dummy_vector.data();

        // get a timestamp for this trigger
        int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                         system_clock::now().time_since_epoch())
                         .count();

        // create TriggerRecordHeader
        dunedaq::daqdataformats::TriggerRecordHeaderData trh_data;
        trh_data.trigger_number = trig_num;
        trh_data.trigger_timestamp = ts;
        trh_data.num_requested_components = components_per_record;
        trh_data.run_number = run_number;
        trh_data.sequence_number = 0;
        trh_data.max_sequence_number = 1;
        trh_data.element_id = dunedaq::daqdataformats::SourceID(
            dunedaq::daqdataformats::SourceID::Subsystem::kTRBuilder, 0);

        dunedaq::daqdataformats::TriggerRecordHeader trh(&trh_data);

        // create our TriggerRecord
        // dunedaq::daqdataformats::TriggerRecord tr(trh);
        // dunedaq::datafilter::trigger_record_ptr_t tr(&trh);
        auto tr = std::make_unique<dunedaq::daqdataformats::TriggerRecord>(trh);

        // loop over elements tpc
        for (size_t ele_num = 0; ele_num < element_count_tpc; ++ele_num) {
            // create our fragment
            dunedaq::daqdataformats::FragmentHeader fh;
            fh.trigger_number = trig_num;
            fh.trigger_timestamp = ts;
            fh.window_begin = ts;
            fh.window_end = ts;
            fh.run_number = run_number;
            fh.fragment_type =
                static_cast<dunedaq::daqdataformats::fragment_type_t>(
                    dunedaq::daqdataformats::FragmentType::kWIB);
            fh.sequence_number = 0;
            fh.detector_id = static_cast<uint16_t>(
                dunedaq::detdataformats::DetID::Subdetector::kHD_TPC);
            fh.element_id = dunedaq::daqdataformats::SourceID(
                dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
                ele_num);

            std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
                new dunedaq::daqdataformats::Fragment(dummy_data,
                                                      fragment_size));
            frag_ptr->set_header_fields(fh);

            // add fragment to TriggerRecord
            tr->add_fragment(std::move(frag_ptr));

        }  // end loop over elements

        // loop over elements pds
        for (size_t ele_num = 0; ele_num < element_count_pds; ++ele_num) {
            // create our fragment
            dunedaq::daqdataformats::FragmentHeader fh;
            fh.trigger_number = trig_num;
            fh.trigger_timestamp = ts;
            fh.window_begin = ts;
            fh.window_end = ts;
            fh.run_number = run_number;
            fh.fragment_type =
                static_cast<dunedaq::daqdataformats::fragment_type_t>(
                    dunedaq::daqdataformats::FragmentType::kDAPHNE);
            fh.sequence_number = 0;
            fh.detector_id = static_cast<uint16_t>(
                dunedaq::detdataformats::DetID::Subdetector::kHD_PDS);
            fh.element_id = dunedaq::daqdataformats::SourceID(
                dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
                ele_num + element_count_tpc);

            std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
                new dunedaq::daqdataformats::Fragment(dummy_data,
                                                      fragment_size));
            frag_ptr->set_header_fields(fh);

            // add fragment to TriggerRecord
            // tr.add_fragment(std::move(frag_ptr));
            tr->add_fragment(std::move(frag_ptr));

        }  // end loop over elements

        // loop over TriggerActivity
        for (size_t ele_num = 0; ele_num < element_count_ta; ++ele_num) {
            // create our fragment
            dunedaq::daqdataformats::FragmentHeader fh;
            fh.trigger_number = trig_num;
            fh.trigger_timestamp = ts;
            fh.window_begin = ts;
            fh.window_end = ts;
            fh.run_number = run_number;
            fh.fragment_type =
                static_cast<dunedaq::daqdataformats::fragment_type_t>(
                    dunedaq::daqdataformats::FragmentType::kTriggerActivity);
            fh.sequence_number = 0;
            fh.detector_id = static_cast<uint16_t>(
                dunedaq::detdataformats::DetID::Subdetector::kDAQ);
            fh.element_id = dunedaq::daqdataformats::SourceID(
                dunedaq::daqdataformats::SourceID::Subsystem::kTrigger,
                ele_num);

            std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
                new dunedaq::daqdataformats::Fragment(dummy_data,
                                                      fragment_size));
            frag_ptr->set_header_fields(fh);

            // add fragment to TriggerRecord
            tr->add_fragment(std::move(frag_ptr));

        }  // end loop over elements

        // loop over TriggerCandidate
        for (size_t ele_num = 0; ele_num < element_count_tc; ++ele_num) {
            // create our fragment
            dunedaq::daqdataformats::FragmentHeader fh;
            fh.trigger_number = trig_num;
            fh.trigger_timestamp = ts;
            fh.window_begin = ts;
            fh.window_end = ts;
            fh.run_number = run_number;
            fh.fragment_type =
                static_cast<dunedaq::daqdataformats::fragment_type_t>(
                    dunedaq::daqdataformats::FragmentType::kTriggerCandidate);
            fh.sequence_number = 0;
            fh.detector_id = static_cast<uint16_t>(
                dunedaq::detdataformats::DetID::Subdetector::kDAQ);
            fh.element_id = dunedaq::daqdataformats::SourceID(
                dunedaq::daqdataformats::SourceID::Subsystem::kTrigger,
                ele_num + element_count_ta);

            std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
                new dunedaq::daqdataformats::Fragment(dummy_data,
                                                      fragment_size));
            frag_ptr->set_header_fields(fh);

            // add fragment to TriggerRecord
            tr->add_fragment(std::move(frag_ptr));

        }  // end loop over elements

        dunedaq::datafilter::trigger_record_ptr_t temp = std::move(tr);
        return temp;
    }

    void send_tr_from_hdf5file(size_t dataflow_run_number,
                               pid_t subscriber_pid) {
        std::ostringstream ss;
        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "TR_tracking2");
        std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
        std::mutex tracking_mutex;

        for (size_t group = 0; group < config.num_groups; ++group) {
            for (size_t conn = 0; conn < config.num_connections_per_group;
                 ++conn) {
                auto info = std::make_shared<TRWriterInfo>(group, conn);
                // auto info = std::make_shared<TRWriterInfo>(0, 0);
                trwriters.push_back(info);
            }
        }

        TLOG_DEBUG(7) << "Getting publisher objects for each connection";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters), [=](std::shared_ptr<TRWriterInfo> info) {
                auto before_sender = std::chrono::steady_clock::now();
                //                    info->sender =
                //                    dunedaq::get_iom_sender<dunedaq::datafilter::Data>(
                info->sender = dunedaq::get_iom_sender<
                    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
                    config.get_connection_name(config.my_id, info->group_id,
                                               info->conn_id));
                auto after_sender = std::chrono::steady_clock::now();
                info->get_sender_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_sender - before_sender);
            });

        TLOG_DEBUG(7) << "Starting publish threads";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters),
            [=, &completed_receiver_tracking,
             &tracking_mutex](std::shared_ptr<TRWriterInfo> info) {
                info->send_thread.reset(new std::thread(
                    [=, &completed_receiver_tracking, &tracking_mutex]() {
                        bool complete_received = false;

                        std::ostringstream ss;
                        std::this_thread::sleep_for(100ms);
                        while (!complete_received) {
                            TLOG() << "Sender message: generate trigger "
                                      "record";

                            //                            std::string
                            //                            input_h5_filename1 =
                            //                                "/lcg/storage19/test-area/dune/trigger_records/"
                            //                                "swtest_run001039_0000_dataflow0_datawriter_0_"
                            //                                "20231103T121050.hdf5";
                            std::string ifilename = config.input_h5_filename;
                            HDF5RawDataFile h5_file(ifilename);
                            auto records = h5_file.get_all_record_ids();
                            ss << "\nNumber of records: " << records.size();
                            if (records.empty()) {
                                ss << "\n\nNO TRIGGER RECORDS FOUND";
                                TLOG() << ss.str();
                                exit(0);
                            }
                            auto first_rec = *(records.begin());
                            auto last_rec = *(
                                std::next(records.begin(), records.size() - 1));

                            ss << "\n\tFirst trigger record: "
                               << first_rec.first << "," << first_rec.second;
                            ss << "\n\tLast trigger record: " << last_rec.first
                               << "," << last_rec.second;

                            TLOG() << ss.str();
                            ss.str("");

                            for (auto const& rid : records) {
                                auto record_header_dataset =
                                    h5_file.get_record_header_dataset_path(rid);
                                auto tr = h5_file.get_trigger_record(rid);

                                // SERIALIZE
                                auto bytes = dunedaq::serialization::serialize(
                                    tr, dunedaq::serialization::kMsgPack);
                                // DESERIALIZE
                                auto deserialized =
                                    dunedaq::serialization::deserialize<
                                        std::unique_ptr<TriggerRecord>>(bytes);

                                // dunedaq::datafilter::trigger_record_ptr_t
                                //     temp_record(&tr);

                                info->sender->try_send(
                                    std::move(deserialized),
                                    std::chrono::milliseconds(50));
                            }

                            ++info->messages_sent;
                            {
                                std::lock_guard<std::mutex> lk(tracking_mutex);
                                if ((completed_receiver_tracking.count(
                                         info->group_id) &&
                                     completed_receiver_tracking[info->group_id]
                                         .count(info->conn_id)) ||
                                    completed_receiver_tracking.count(-1)) {
                                    TLOG() << "Complete_received";
                                    complete_received = true;
                                }
                            }
                            complete_received = true;
                            break;
                        }  // while loop
                    }));
            });

        TLOG_DEBUG(7) << "Joining send threads";
        for (auto& sender : trwriters) {
            sender->send_thread->join();
            sender->send_thread.reset(nullptr);
        }
    }

    void send_tr(size_t dataflow_run_number, pid_t subscriber_pid) {
        std::ostringstream ss;

        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "TR_tracking2");
        std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
        std::mutex tracking_mutex;

        //    for (size_t group = 0; group < config.num_groups; ++group) {
        //      for (size_t conn = 0; conn <
        //      config.num_connections_per_group;
        //      ++conn) {
        // auto info = std::make_shared<TRWriterInfo>(group, conn);
        auto info = std::make_shared<TRWriterInfo>(0, 0);
        trwriters.push_back(info);
        //      }
        //    }

        TLOG_DEBUG(7) << "Getting publisher objects for each connection";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters), [=](std::shared_ptr<TRWriterInfo> info) {
                auto before_sender = std::chrono::steady_clock::now();
                //                    info->sender =
                //                    dunedaq::get_iom_sender<dunedaq::datafilter::Data>(
                info->sender = dunedaq::get_iom_sender<
                    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
                    config.get_connection_name(config.my_id, info->group_id,
                                               info->conn_id));
                auto after_sender = std::chrono::steady_clock::now();
                info->get_sender_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_sender - before_sender);
            });

        TLOG_DEBUG(7) << "Starting publish threads";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters),
            [=, &completed_receiver_tracking,
             &tracking_mutex](std::shared_ptr<TRWriterInfo> info) {
                info->send_thread.reset(new std::thread(
                    [=, &completed_receiver_tracking, &tracking_mutex]() {
                        bool complete_received = false;

                        std::this_thread::sleep_for(100ms);
                        while (!complete_received) {
                            TLOG() << "Sender message: generate trigger "
                                      "record";
                            dunedaq::datafilter::trigger_record_ptr_t
                                temp_record(create_trigger_record(1));

                            TLOG() << "Start sending  trigger record";
                            info->sender->try_send(
                                std::move(temp_record),
                                std::chrono::milliseconds(
                                    config.send_interval_ms));
                            TLOG() << "End sending trigger record";
                            ++info->messages_sent;
                            {
                                std::lock_guard<std::mutex> lk(tracking_mutex);
                                if ((completed_receiver_tracking.count(
                                         info->group_id) &&
                                     completed_receiver_tracking[info->group_id]
                                         .count(info->conn_id)) ||
                                    completed_receiver_tracking.count(-1)) {
                                    TLOG() << "Complete_received";
                                    complete_received = true;
                                }
                            }
                            std::this_thread::sleep_for(1000ms);
                            complete_received = true;
                            break;
                        }  // while loop
                    }));
            });

        TLOG_DEBUG(7) << "Joining send threads";
        for (auto& sender : trwriters) {
            sender->send_thread->join();
            sender->send_thread.reset(nullptr);
        }
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
    bool is_hdf5file = false;

    namespace po = boost::program_options;
    po::options_description desc("data filter trigger records send test.");
    desc.add_options()("input-file,f",
                       po::value<std::string>(&config.input_h5_filename)
                           ->default_value(config.input_h5_filename),
                       "input filename ")(
        "hdf5,h5", po::bool_switch(&is_hdf5file), "toggle to hdf5 file")(
        "help,h", po::bool_switch(&help_requested), "For help.");

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
        // trwriter->send(run, forked_pids[0]);
        if (is_hdf5file) {
            trwriter->send_tr_from_hdf5file(run, 0);
        } else {
            trwriter->send_tr(run, 0);
        }
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
