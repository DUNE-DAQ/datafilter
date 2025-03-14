#include <sys/wait.h>

#include <algorithm>
#include <execution>
#include <fstream>
#include <string>
#include <thread>

#include "boost/program_options.hpp"
#include "datafilter/app/Nljs.hpp"
#include "datafilter/app/Structs.hpp"
#include "datafilter/data_struct.hpp"
#include "detdataformats/DetID.hpp"
#include "dfmessages/TriggerRecord_serialization.hpp"
#include "dfmessages/Types.hpp"
#include "dfmodules/DataStore.hpp"
#include "dfmodules/datawriter/Nljs.hpp"
#include "dfmodules/datawriterinfo/InfoNljs.hpp"
#include "dfmodules/hdf5datastore/Nljs.hpp"
#include "dfmodules/hdf5datastore/Structs.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/hdf5filelayout/Nljs.hpp"
#include "hdf5libs/hdf5rawdatafile/Nljs.hpp"
#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::detdataformats;
using namespace dunedaq::iomanager;
using namespace dunedaq::dfmodules;
using namespace dunedaq::dfmessages;

namespace dunedaq {
namespace datafilter {

using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

struct DataFilterConfig {
    bool use_connectivity_service = false;

    int port = 5000;
    int portA = 15500;  // for trdispatcher/datafilter connection
    int portB = 15501;  // for datafilter/trwriter connection

    std::string server = "127.0.0.1";
    std::string server_trdispatcher = "127.0.0.1";
    std::string info_file_base = "datafilter";
    std::string session_name = "datafilter test run";
    size_t num_apps = 1;
    size_t num_connections_per_group = 1;
    size_t num_groups = 1;
    size_t num_messages = 1;
    size_t message_size_kb = 1024;
    size_t num_runs = 1;
    size_t my_id1 = 0;
    size_t my_id2 = 1;
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
                                  size_t conn_id, size_t port) {
        assert(num_apps < 253);
        assert(num_groups < 253);
        assert(num_connections_per_group < 252);

        int first_byte = conn_id + 2;    // 2-254
        int second_byte = group_id + 1;  // 1-254
        int third_byte = app_id + 1;     // 1-254
        std::string conn_addr;
        if (server == "127.0.0.1") {
            conn_addr = "tcp://127." + std::to_string(third_byte) + "." +
                        std::to_string(second_byte) + "." +
                        std::to_string(first_byte) + ":" + std::to_string(port);
        } else {
            conn_addr = "tcp://" + server + ":" + std::to_string(port);
        }

        return conn_addr;
    }

    std::string get_subscriber_init_name() {
        return get_subscriber_init_name(my_id1);
    }
    std::string get_subscriber_init_name(size_t id) {
        return "conn_init_" + std::to_string(id);
    }
    // std::string get_publisher_init_name() { return "conn_init_.*"; }

    void configure_iomanager() {
        setenv("DUNEDAQ_PARTITION", session_name.c_str(), 0);

        Queues_t queues;
        Connections_t connections;

        //        for (size_t group = 0; group < num_groups; ++group) {
        //            for (size_t conn = 0; conn < num_connections_per_group;
        //            ++conn) {
        //                auto conn_addr1 = get_connection_ip(my_id1, group,
        //                conn, portA); auto conn_addr2 =
        //                get_connection_ip(my_id2, group, conn, portB); TLOG()
        //                << "Adding connection with id "
        //                       << get_connection_name(my_id1, group, conn)
        //                       << " and address1 " << conn_addr1;
        //                TLOG() << "Adding connection with id "
        //                       << get_connection_name(my_id2, group, conn)
        //                       << " and address2 " << conn_addr2;
        //                // data between dispatcher and data filter.
        //                connections.emplace_back(Connection{
        //                    ConnectionId{get_connection_name(my_id1, group,
        //                    conn),
        //                                 //             "data_t"},
        //                                 "TriggerRecord"},
        //                    conn_addr1, ConnectionType::kPubSub});
        //                // data between data filter and filter results writer.
        //                connections.emplace_back(Connection{
        //                    ConnectionId{get_connection_name(my_id2, group,
        //                    conn),
        //                                 "TriggerRecord"},
        //                    conn_addr2, ConnectionType::kPubSub});
        //            }
        //        }
        //
        auto conn_addr0 = "tcp://" + server + ":" + std::to_string(portA);
        auto conn_addr1 = "tcp://" + server + ":" + std::to_string(portB);
        connections.emplace_back(
            Connection{ConnectionId{"conn_A0_G0_C0_", "TriggerRecord"},
                       // conn_addr0, ConnectionType::kPubSub});
                       conn_addr0, ConnectionType::kSendRecv});
        connections.emplace_back(
            Connection{ConnectionId{"conn_A1_G0_C0_", "TriggerRecord"},
                       // conn_addr1, ConnectionType::kPubSub});
                       conn_addr1, ConnectionType::kSendRecv});

        //  for (size_t sub = 0; sub < num_apps; ++sub) {
        for (size_t sub = 0; sub < 3; ++sub) {
            auto port = 13000 + sub;
            std::string conn_addr =
                "tcp://" + server + ":" + std::to_string(port);
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

        //  for (size_t sub = 0; sub < num_apps; ++sub) {
        for (size_t sub = 0; sub < 3; ++sub) {
            auto port = 23000 + sub;
            std::string conn_addr =
                "tcp://" + server_trdispatcher + ":" + std::to_string(port);
            TLOG() << "Adding control connection "
                   << "trdispatcher" + std::to_string(sub) << " with address "
                   << conn_addr;

            connections.emplace_back(
                // Connection{ ConnectionId{ "TR_tracking"+std::to_string(sub),
                // "init_t" }, conn_addr, ConnectionType::kPubSub });
                Connection{ConnectionId{"trdispatcher" + std::to_string(sub),
                                        "init_t"},
                           conn_addr, ConnectionType::kSendRecv});
        }

        //      for (size_t sub = 0; sub < num_apps; ++sub) {
        for (size_t sub = 0; sub < 3; ++sub) {
            auto port = 33000 + sub;
            std::string conn_addr =
                "tcp://" + server + ":" + std::to_string(port);
            TLOG() << "Adding control connection "
                   << "trwriter" + std::to_string(sub) << " with address "
                   << conn_addr;

            connections.emplace_back(
                // Connection{ ConnectionId{ "TR_tracking"+std::to_string(sub),
                // "init_t" }, conn_addr, ConnectionType::kPubSub });
                Connection{
                    ConnectionId{"trwriter" + std::to_string(sub), "init_t"},
                    conn_addr, ConnectionType::kSendRecv});
        }

        // Create BookKeeping socket
        auto port = 83000;
        auto sub = 0;
        std::string conn_addrbookkeeping =
            "tcp://" + server + ":" + std::to_string(port);
        TLOG() << "Adding control connection "
               << "bookkeeping" + std::to_string(sub) << " with address "
               << conn_addrbookkeeping;

        connections.emplace_back(Connection{
            ConnectionId{"bookkeeping" + std::to_string(sub), "bk_t"},
            conn_addrbookkeeping, ConnectionType::kSendRecv});

        IOManager::get()->configure(
            queues, connections, use_connectivity_service,
            std::chrono::milliseconds(publish_interval));
    }
};

struct DataFilterReceiver {};

struct TRRewriter {
    TRRewriter() {
        setenv("DUNEDAQ_PARTITION", "IOManager_t", 0);

        std::cout << "from TRRewriter";
    }
    ~TRRewriter() { IOManager::get()->reset(); }
    explicit TRRewriter(dunedaq::datafilter::DataFilterConfig c) : config(c) {}

    TRRewriter(TRRewriter const&) = default;
    TRRewriter(TRRewriter&&) = default;
    TRRewriter& operator=(TRRewriter const&) = default;
    TRRewriter& operator=(TRRewriter&&) = default;

    std::string session_name = "iomanager : TRRewriter test";
    bool use_connectivity_service = false;  // unsed for now
    int publish_interval = 1000;
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
        std::shared_ptr<SenderConcept<trigger_record_ptr_t>> sender;
        // std::shared_ptr<SenderConcept<daqdataformats::TriggerRecord>> sender;
        std::unique_ptr<std::thread> send_thread;
        std::chrono::milliseconds get_sender_time;

        TRWriterInfo(size_t group, size_t conn)
            : conn_id(conn), group_id(group) {}
    };

    size_t fragment_size = 100;
    size_t element_count_tpc = 4;
    size_t element_count_pds = 4;
    size_t element_count_ta = 4;
    size_t element_count_tc = 1;
    const size_t components_per_record = element_count_tpc + element_count_pds +
                                         element_count_ta + element_count_tc;
    int run_number = 53;
    int file_index = 0;

    std::vector<std::shared_ptr<TRWriterInfo>> trwriters;
    DataFilterConfig config;

    void init(const data_t& init_data) {
        TLOG_DEBUG(5) << "Getting init sender";
        // auto init_sender =
        // dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(config.get_pub_init_name());
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
            // Handshake q(config.my_id, -1, 0, run_number);
            dunedaq::datafilter::Handshake q("start");
            init_sender->send(std::move(q), Sender::s_block);
            dunedaq::datafilter::Handshake recv;
            recv = init_receiver->receive(std::chrono::milliseconds(100));
            std::this_thread::sleep_for(100ms);
            if (recv.msg_id == "gotit") TLOG() << "Receiver got it";
            break;
        }
    }
    dunedaq::datafilter::trigger_record_ptr_t create_trigger_record(
        uint64_t trig_num) {
        // test setup our dummy_data
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
        auto tr = std::make_unique<TriggerRecord>(trh);

        // loop over elements tpc
        for (size_t ele_num = 0; ele_num < element_count_tpc; ++ele_num) {
            // create our fragment
            dunedaq::daqdataformats::FragmentHeader fh;
            fh.trigger_number = trig_num;
            fh.trigger_timestamp = ts;
            fh.window_begin = ts - 10;
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

    struct TriggerId {
        TriggerId() = default;

        //  explicit TriggerId(const dfmessages::TriggerDecision& td,
        //                     daqdataformats::sequence_number_t s =
        //                     daqdataformats::TypeDefaults::s_invalid_sequence_number)
        //    : trigger_number(td.trigger_number)
        //    , sequence_number(s)
        //    , run_number(td.run_number)
        //  {
        //    ;
        //  }
        explicit TriggerId(daqdataformats::Fragment& f)
            : trigger_number(f.get_trigger_number()),
              sequence_number(f.get_sequence_number()),
              run_number(f.get_run_number()) {
            ;
        }

        daqdataformats::trigger_number_t trigger_number;
        daqdataformats::sequence_number_t sequence_number;
        daqdataformats::run_number_t run_number;

        bool operator<(const TriggerId& other) const noexcept {
            return std::tuple(trigger_number, sequence_number, run_number) <
                   std::tuple(other.trigger_number, other.sequence_number,
                              other.run_number);
        }

        friend std::ostream& operator<<(std::ostream& out,
                                        const TriggerId& id) noexcept {
            out << id.trigger_number << '-' << id.sequence_number << '/'
                << id.run_number;
            return out;
        }

        friend TraceStreamer& operator<<(TraceStreamer& out,
                                         const TriggerId& id) noexcept {
            return out << id.trigger_number << '.' << id.sequence_number << "/"
                       << id.run_number;
        }

        friend std::istream& operator>>(std::istream& in, TriggerId& id) {
            char t1, t2;
            in >> id.trigger_number >> t1 >> id.sequence_number >> t2 >>
                id.run_number;
            return in;
        }
    };

    trigger_record_ptr_t extract_trigger_record(const TriggerId& id) {
        using clock_type = std::chrono::high_resolution_clock;
        std::map<TriggerId,
                 std::pair<clock_type::time_point, trigger_record_ptr_t>>
            m_trigger_records;
        m_trigger_records.clear();
        // using metric_counter_type =
        // decltype(triggerrecordbuilderinfo::Info::pending_trigger_decisions);
        using metric_counter_type = uint64_t;
        std::atomic<metric_counter_type> m_trigger_decisions_counter = {
            0};                                                     // currently
        std::atomic<metric_counter_type> m_fragment_counter = {0};  // currently
        std::atomic<metric_counter_type> m_pending_fragment_counter = {
            0};  // currently

        std::atomic<metric_counter_type> m_timed_out_trigger_records = {
            0};  // in the run
        std::atomic<metric_counter_type> m_unexpected_fragments = {
            0};  // in the run
        std::atomic<metric_counter_type> m_unexpected_trigger_decisions = {
            0};                                                   // in the run
        std::atomic<metric_counter_type> m_lost_fragments = {0};  // in the run
        std::atomic<metric_counter_type> m_invalid_requests = {
            0};  // in the run
        std::atomic<metric_counter_type> m_duplicated_trigger_ids = {
            0};  // in the run
        std::atomic<metric_counter_type> m_abandoned_trigger_records = {
            0};  // in the run

        std::atomic<metric_counter_type> m_received_trigger_decisions = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_generated_trigger_records = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_generated_data_requests = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_sleep_counter = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_loop_counter = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_data_waiting_time = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_trigger_decision_width = {
            0};  // in between calls
        std::atomic<metric_counter_type> m_data_request_width = {
            0};  // in between calls

        std::atomic<metric_counter_type> m_trmon_request_counter = {0};
        std::atomic<metric_counter_type> m_trmon_sent_counter = {0};

        auto it = m_trigger_records.find(id);

        trigger_record_ptr_t temp = std::move(it->second.second);

        auto time = clock_type::now();
        auto duration = time - it->second.first;

        // m_data_waiting_time +=
        // std::chrono::duration_cast<duration_type>(duration).count();

        m_trigger_records.erase(it);

        --m_trigger_decisions_counter;
        m_fragment_counter -= temp->get_fragments_ref().size();

        auto missing_fragments =
            temp->get_header_ref().get_num_requested_components() -
            temp->get_fragments_ref().size();

        if (missing_fragments > 0) {
            m_lost_fragments += missing_fragments;
            m_pending_fragment_counter -= missing_fragments;
            temp->get_header_ref().set_error_bit(
                TriggerRecordErrorBits::kIncomplete, true);

            TLOG()
                << " sending incomplete TriggerRecord downstream at Stop time "
                << "(trigger/run_number=" << id << ", "
                << temp->get_fragments_ref().size() << " of "
                << temp->get_header_ref().get_num_requested_components()
                << " fragments included)";
        }

        return temp;
    }

    void send_tr(trigger_record_ptr_t& trp) {
        std::stringstream ss;
        ss << "datafilter: ->accepted_trigger_record2->send_tr :Sending TR to FilterResultWriter";
        TLOG() << ss.str();
        ss.str("");

        auto init_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                "trwriter0");
        dunedaq::datafilter::Handshake q("write_tr");
        init_sender->send(std::move(q), Sender::s_block);

        std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
        std::mutex tracking_mutex;

        //    for (size_t group = 0; group < config.num_groups; ++group) {
        //      for (size_t conn = 0; conn < config.num_connections_per_group;
        //      ++conn) {
        // auto info = std::make_shared<TRWriterInfo>(group, conn);
        auto info = std::make_shared<TRWriterInfo>(0, 0);
        trwriters.push_back(info);
        //      }
        //    }
        auto trigger_timestamp =
            trp->get_fragments_ref().at(0)->get_trigger_timestamp();
        auto trigger_number =
            trp->get_fragments_ref().at(0)->get_trigger_number();
        auto run_number = trp->get_fragments_ref().at(0)->get_run_number();

        TLOG() << "datafilter->send_tr: run_number: " << run_number
               << ", trigger number: " << trigger_number;

        TLOG_DEBUG(7) << "Getting publisher objects for each connection";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters), [=](std::shared_ptr<TRWriterInfo> info) {
                auto before_sender = std::chrono::steady_clock::now();
                info->sender = dunedaq::get_iom_sender<trigger_record_ptr_t>(
                    config.get_connection_name(config.my_id2, info->group_id,
                                               info->conn_id));
                auto after_sender = std::chrono::steady_clock::now();
                info->get_sender_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_sender - before_sender);
            });

        TLOG() << "Starting publish threads to connect with FilterResultWriter";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters),
            [=, &completed_receiver_tracking, &tracking_mutex,
             &trp](std::shared_ptr<TRWriterInfo> info) {
                info->send_thread.reset(new std::thread(
                    [=, &completed_receiver_tracking, &tracking_mutex, &trp]() {
                        bool complete_received = false;

                        std::this_thread::sleep_for(100ms);
                        while (!complete_received) {
                            info->sender->try_send(
                                std::move(trp), std::chrono::milliseconds(
                                                    config.send_interval_ms));

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

                            std::this_thread::sleep_for(500ms);
                            complete_received = true;
                            break;
                        }  // while loop
                    }));
            });

        TLOG() << "datafilter send_tr: Joining send threads";
        for (auto& sender : trwriters) {
            sender->send_thread->join();
            sender->send_thread.reset(nullptr);
        }
    }

    void send_tr2() {
        std::stringstream ss;

        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "trwriter0");
        std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
        std::mutex tracking_mutex;

        //    for (size_t group = 0; group < config.num_groups; ++group) {
        //      for (size_t conn = 0; conn < config.num_connections_per_group;
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
                info->sender = dunedaq::get_iom_sender<trigger_record_ptr_t>(
                    config.get_connection_name(config.my_id2, info->group_id,
                                               info->conn_id));
                auto after_sender = std::chrono::steady_clock::now();
                info->get_sender_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_sender - before_sender);
            });

        TLOG() << "Starting publish threads";
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
                            TLOG() << "Sender message: generate trigger record";
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

    void send_trigger_record() {
        // std::unique_ptr<dunedaq::daqdataformats::TriggerRecord> tr1;
        TriggerRecordHeaderData trh_data;
        //      record_header.set_trigger_number(1);
        //      record_header.set_trigger_timestamp(2);
        //      record_header.set_run_number(3);
        //      record_header.set_trigger_type(4);
        //      record_header.set_sequence_number(5);
        //      record_header.set_max_sequence_number(6);
        trh_data.trigger_number = 1;
        trh_data.trigger_timestamp = 2;
        trh_data.run_number = 3;
        trh_data.sequence_number = 4;
        // trh_data.max_sequence_number = max_seq_num;

        TriggerRecordHeader trh1(&trh_data);
        // create out TriggerRecord
        TriggerRecord tr1(trh1);

        TLOG() << "send trigger record to trwriter";
        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "trwriter0");
        std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
        std::mutex tracking_mutex;

        auto info = std::make_shared<TRWriterInfo>(0, 0);
        trwriters.push_back(info);

        // TriggerId& id;
        // id.trigger_number=1;
        // id.sequence_number=1;
        //        auto iom = iomanager::IOManager::get();
        //        do {
        //            try {
        //                iom->get_iom_sender<trigger_record_ptr>(it->)
        //            }
        //        }
        //
        TLOG() << "Getting TRWriter objects for each connection";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters), [=](std::shared_ptr<TRWriterInfo> info) {
                auto before_sender = std::chrono::steady_clock::now();
                info->sender = dunedaq::get_iom_sender<trigger_record_ptr_t>(
                    config.get_connection_name(config.my_id2, info->group_id,
                                               info->conn_id));
                auto after_sender = std::chrono::steady_clock::now();
                info->get_sender_time =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        after_sender - before_sender);
            });

        TLOG_DEBUG(7) << "Starting TRWriter threads";
        std::for_each(
            std::execution::par_unseq, std::begin(trwriters),
            std::end(trwriters),
            [=, &completed_receiver_tracking,
             &tracking_mutex](std::shared_ptr<TRWriterInfo> info) {
                info->send_thread.reset(new std::thread(
                    [=, &completed_receiver_tracking, &tracking_mutex]() {
                        bool complete_received = false;

                        // std::vector<TriggerId> complete;
                        // for (const auto& id : complete) {
                        std::optional<std::unique_ptr<daqdataformats::Fragment>>
                            temp_fragment;
                        temp_fragment.value()->set_trigger_number(4);
                        //          temp_fragment.value()->set_run_number(1);
                        //          temp_fragment.value()->set_sequence_number(1);
                        //          TriggerId
                        //          id(*temp_fragment.value());

                        //          trigger_record_ptr_t
                        //          temp_record(extract_trigger_record(id));
                        //}

                        //       auto trigger_record_bytes =
                        //                         serialization::serialize(temp_record,
                        //                         serialization::SerializationType::kMsgPack);
                        //       trigger_record_ptr_t record_copy =
                        //       serialization::deserialize<trigger_record_ptr_t>(trigger_record_bytes);

                        //             while (!complete_received) {
                        //                 //info->sender->try_send(std::move(temp_record),
                        //                 std::chrono::milliseconds(config.send_interval_ms));
                        //                 info->sender->try_send(std::move(temp_record),
                        //                 std::chrono::milliseconds(config.send_interval_ms));
                        ////
                        /// info->sender->send(std::move(tr1),iomanager::Sender::s_no_block);
                        //                 ++info->messages_sent;
                        //                 {
                        //                   std::lock_guard<std::mutex>
                        //                   lk(tracking_mutex); if
                        //                   ((completed_receiver_tracking.count(info->group_id)
                        //                   &&
                        //                        completed_receiver_tracking[info->group_id].count(info->conn_id))
                        //                        ||
                        //                       completed_receiver_tracking.count(-1))
                        //                       {
                        //                     complete_received = true;
                        //                   }
                        //                 }
                        //             }
                        //
                    }));
            });
    }
};

struct DataFilterMonitor {
    void get_info() { opmonlib::InfoCollector ci; }
};

// struct Bookkeeping
//{
//     //central collection of bookkeeping data
//     request_number_t request_number{ TypeDefaults::s_invalid_request_number
//     }; trigger_type_t trigger_type{ TypeDefaults::s_invalid_trigger_type };
//     request_number{ TypeDefaults::s_invalid_request_number };
//     std::string book_data_from;
// }
//
struct DataFilterOrganiser {
    // DataFilterConfig config;
    // DataFilterOrganiser(DataFilterConfig c) :config(c)
    //{}
    TRRewriter rewriter;
    DataFilterMonitor monitor;

    // create TriggerRecordHeader
    TriggerRecordHeaderData trh_data;

    uint32_t nchannels = 64;
    uint32_t nsamples = 64;

    dunedaq::daqdataformats::TriggerRecord rebuild_trigger_record(
        size_t trigger_number, size_t trigger_timestamp, size_t run_number,
        size_t seq_number, size_t n_frames, size_t element_id,
        size_t detector_id, std::vector<int> contents) {
        trh_data.trigger_number = trigger_number;
        trh_data.trigger_timestamp = trigger_timestamp;
        // trh_data.num_requested_components = num_requested_components;
        trh_data.run_number = run_number;
        trh_data.sequence_number = seq_number;
        // trh_data.max_sequence_number = max_seq_num;

        TriggerRecordHeader trh1(&trh_data);
        // create out TriggerRecord
        TriggerRecord tr1(trh1);
        //        std::unique_ptr<daqdataformats::TriggerRecord> & tr_ptr;
        //        // create our fragment
        //        FragmentHeader fh;
        //        fh.trigger_number = msg.trigger_number;
        //        fh.trigger_timestamp = msg.trigger_timestamp;
        //        fh.window_begin = msg.trigger_timestamp - 10;
        //        fh.window_end = msg.trigger_timestamp;
        //        fh.run_number = msg.run_number;
        //        fh.fragment_type = msg.fragment_type;
        //        fh.sequence_number = msg.seq_number;
        //        //fh.element_id = GeoID(gtype_to_use, reg_num, ele_num);
        //        //fh.element_id = elem_id;
        //
        std::vector<std::vector<uint16_t>> vec;
        dunedaq::fddetdataformats::WIBEthFrame frame{};
        for (auto i = 0; i < n_frames; ++i) {
            // auto frame =
            // reinterpret_cast<fddetdataformats::WIBEthFrame*>(static_cast<char*>(data)
            // + i * sizeof(fddetdataformats::WIBEthFrame));
            vec.emplace_back(std::vector<uint16_t>(n_frames));
            if (i < 20) TLOG() << "Receiver: contents " << contents[i];
            for (auto j = 0; j < nchannels; ++j) {
                // frame->set_adc(j,nsamples,msg.contents[i]);
                frame.set_adc(j, nsamples, contents[i]);
                vec[i][j] = frame.get_adc(j, nsamples);
            }
        }

        std::vector<std::pair<void*, size_t>> list_of_pieces;
        std::unique_ptr<Fragment> frag(new Fragment(list_of_pieces));
        // std::unique_ptr<Fragment> frag{};
        // frag.reset(new Fragment(vec));

        // this is another way to set the fragment header
        // frag->set_type(msg.fragment_type);
        frag->set_run_number(run_number);
        frag->set_trigger_number(trigger_number);
        frag->set_window_begin(trigger_timestamp - 10);
        frag->set_window_end(trigger_timestamp);
        frag->set_element_id(dunedaq::daqdataformats::SourceID(
            dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
            element_id));
        frag->set_detector_id(detector_id);
        // frag->set_type(daqdataformats::FragmentType::kTriggerPrimitives);

        tr1.add_fragment(std::move(frag));
        // auto data = frag->get_data();

        return tr1;
    }

    void accepted_trigger_record(size_t trigger_number,
                                 size_t trigger_timestamp, size_t run_number,
                                 size_t seq_number, size_t n_frames,
                                 size_t element_id, size_t detector_id,
                                 std::vector<int> contents) {
        TLOG() << "====>accepted_trigger_record using frames method";
        hdf5datastore::ConfParams conf;
        conf.name = "tempWriter";
        conf.mode = "all-per-file";
        conf.directory_path = "/opt/tmp/chen";
        conf.filename_parameters.writer_identifier = "TRWriter_test";

        std::unique_ptr<DataStore> data_store_ptr;
        data_store_ptr = make_data_store("DatafilterDataStore", conf);
        // data_store_ptr->write(rebuild_trigger_record(
        //     trigger_number, trigger_timestamp, run_number, seq_number,
        //     n_frames,
        //    element_id, detector_id, contents));
        // rewriter.send_tr(rebuild_trigger_record(
        //    trigger_number, trigger_timestamp, run_number, seq_number,
        //    n_frames, element_id, detector_id, contents));
        //  rewriter.send_trigger_record();
    }

    void accepted_trigger_record2(trigger_record_ptr_t& trp) {
        TLOG() << "====>accepted_trigger_record2 single-event per file";
        //        hdf5datastore::ConfParams conf;
        //        conf.name = "tempWriter";
        //        conf.mode = "all-per-file";
        //        conf.directory_path = "/opt/tmp/chen";
        //        conf.filename_parameters.writer_identifier = "TRWriter_test";
        //
        //        std::unique_ptr<DataStore> data_store_ptr;
        //        data_store_ptr = make_data_store("DatafilterDataStore", conf);
        // data_store_ptr->write(*tr);

        auto trigger_timestamp =
            trp->get_fragments_ref().at(0)->get_trigger_timestamp();
        auto trigger_number =
            trp->get_fragments_ref().at(0)->get_trigger_number();
        auto run_number = trp->get_fragments_ref().at(0)->get_run_number();

        TLOG() << "trigger_timestamp " << trigger_timestamp
               << " trigger_number " << trigger_number << " run_number "
               << run_number;

        // data_store_ptr->write(rebuild_trigger_record(
        //     trigger_number, trigger_timestamp, run_number, seq_number,
        //     n_frames,
        //    element_id, detector_id, contents));
        // rewriter.send_tr(rebuild_trigger_record(
        //    trigger_number, trigger_timestamp, run_number, seq_number,
        //    n_frames, element_id, detector_id, contents));
        rewriter.send_tr(trp);
    }

    void send_next_tr() {
        auto init_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                "trdispatcher1");

        dunedaq::datafilter::Handshake sent_t1("next_tr");
        init_sender->send(std::move(sent_t1), Sender::s_block);
    }
};

struct SubscriberTest {
    struct SubscriberInfo {
        size_t group_id;
        size_t conn_id;
        bool is_group_subscriber;
        std::unordered_map<size_t, size_t> last_sequence_received{0};
        std::atomic<size_t> msgs_received{0};
        std::atomic<size_t> msgs_with_error{0};
        std::chrono::milliseconds get_receiver_time;
        std::chrono::milliseconds add_callback_time;
        std::atomic<bool> complete{false};

        size_t total_size_bytes;
        SubscriberInfo(size_t group, size_t conn)
            : group_id(group), conn_id(conn), is_group_subscriber(false) {}
        SubscriberInfo(size_t group)
            : group_id(group), conn_id(0), is_group_subscriber(true) {}

        std::string get_connection_name(DataFilterConfig& config) {
            if (is_group_subscriber) {
                return config.get_group_connection_name(config.my_id1,
                                                        group_id);
            }
            return config.get_connection_name(config.my_id1, group_id, conn_id);
        }
    };
    std::vector<std::shared_ptr<SubscriberInfo>> subscribers;
    DataFilterConfig config;
    DataFilterOrganiser organiser;

    std::queue<nlohmann::json> bk_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;

    explicit SubscriberTest(DataFilterConfig c) : config(c) {}
    uint16_t data3[200000000];
    std::string path_header1;

    void init(size_t datafilter_run_number) {
        TLOG_DEBUG(5) << "Getting init sender";
        auto init_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                //        "trdispatcher0");
                "TR_tracking0");

        std::atomic<std::chrono::steady_clock::time_point> last_received =
            std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - last_received.load())
                   .count() < 500) {
            // Handshake q(config.my_id, -1, 0, run_number);
            TLOG() << "datafilter2";
            dunedaq::datafilter::Handshake recv;
            recv = init_receiver->receive(std::chrono::milliseconds(100));
            TLOG() << "message received:  " << recv.msg_id;
            if (recv.msg_id == "start") break;
            std::this_thread::sleep_for(100ms);
        }

        auto init_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                //        "trdispatcher1");
                "TR_tracking1");
        std::atomic<std::chrono::steady_clock::time_point> last_received1 =
            std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - last_received1.load())
                   .count() < 500) {
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

    nlohmann::json to_json(const BookKeeping& bk) {
        return nlohmann::json{{"entry_id", bk.entry_id},
                              {"conn_id", bk.conn_id},
                              {"from_id", bk.from_id},
                              {"data_filter_id", bk.data_filter_id},
                              {"node", bk.node},
                              {"tr_header_info", bk.tr_header_info},
                              {"tr_status", bk.tr_status},
                              {"file_send_list", bk.file_send_list},
                              {"file_send_status", bk.file_send_status},
                              {"transfer_rate", bk.transfer_rate}};
    }

    // Function to read existing transactions from the file
    nlohmann::json open_existing_bk(const std::string& filename) {
        std::ifstream file(filename);
        if (file.is_open()) {
            try {
                nlohmann::json existing_bk;
                file >> existing_bk;
                return existing_bk;
            } catch (const std::exception& e) {
                std::cerr << "Error reading JSON file: " << e.what()
                          << std::endl;
            }
        }
        return nlohmann::json::array();  // Return an empty array if the file
                                         // doesn't exist or is invalid
    }

    void write_to_file(const std::string& filename,
                       std::atomic<bool>& stop_flag) {
        nlohmann::json existing_bk = open_existing_bk(filename);

        auto start_time = std::chrono::high_resolution_clock::now();
        int transaction_count = 0;

        while (true) {
            std::unique_lock<std::mutex> lock(queue_mutex);

            // Use a lambda to wait for the condition variable
            queue_cv.wait(lock, [&] { return !bk_queue.empty(); });

            nlohmann::json transaction = bk_queue.front();
            bk_queue.pop();
            lock.unlock();

            if (existing_bk.is_array()) {
                existing_bk.push_back(transaction);
            } else {
                std::cerr
                    << "Error: Existing transactions is not an array. Cannot append."
                    << std::endl;
                continue;
            }

            std::ofstream file(filename);
            if (file.is_open()) {
                file << existing_bk.dump(4);
            } else {
                std::cerr << "Failed to open file for writing!" << std::endl;
            }
            transaction_count++;

            if (transaction_count % 1 == 0) {
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();
                std::cout << "Processed " << transaction_count
                          << " transactions in " << duration << " ms"
                          << std::endl;
            }
        }
    }

    void receive_bk() {
        bool bk_done = false;
        std::atomic<unsigned int> received_cnt = 0;
        std::atomic<bool> stop_flag{false};
        std::atomic<unsigned int> run_number;
        std::string bk_file;

        auto cb_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
                "bookkeeping0");

        std::function<void(dunedaq::datafilter::BookKeeping)> str_receiver_cb =
            [&](dunedaq::datafilter::BookKeeping bk) {
                if (bk.entry_id != " ") {
                    // if (bk.bk_info['entry_id'] != " ") {
                    ++received_cnt;
                    // for (auto& item : bk.tr_header_info) {
                    //     if (item.first == "run number") {
                    //         run_number = std::stol(item.second);
                    //     }
                    // }
                    TLOG() << "received_cnt " << received_cnt;
                    // if (received_cnt == 2) {
                    if (bk.from_id == "trdispatcher") {
                        run_number = bk.run_number;
                        bk_file = "bookkeeping" +
                                  std::to_string(bk.run_number) + ".json";
                        TLOG() << "receive_bk " << run_number;
                    } else {
                        run_number = 0;
                        bk_file = "bookkeeping.json";
                    }
                    //}

                    // write_to_file(bk_file, stop_flag);
                    nlohmann::json bk_json = to_json(bk);
                    // nlohmann::json bk_json = bk.bk_info;
                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        bk_queue.push(bk_json);
                    }
                    queue_cv.notify_one();
                }
                TLOG() << "Received new bookkeeping info to store."
                       << bk.entry_id << "from_id " << bk.from_id;
                //                       << bk.bk_info['entry_id'] << "from_id "
                //                       << bk.bk_info['from_id'];
            };

        cb_receiver->add_callback(str_receiver_cb);

        //        std::string bk_file;
        //        if (received_cnt == 2) {
        //            if (run_number != 0) {
        //                bk_file = "bookkeeping" + std::to_string(run_number) +
        //                ".json";
        //            } else {
        //                bk_file = "bookkeeping.json";
        //            }
        //        }
        write_to_file(bk_file, stop_flag);

        while (!bk_done) {
            if (received_cnt == 1) bk_done = true;
        }

        cb_receiver->remove_callback();
        stop_flag.store(true);
        queue_cv.notify_one();
    }

    void receive(size_t run_number1) {
        if (config.next_tr) {
            auto next_tr_sender =
                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
                    //         "trdispatcher2");
                    "TR_tracking2");
            TLOG() << "send next_tr instruction";
            dunedaq::datafilter::Handshake q("next_tr");
            next_tr_sender->send(std::move(q), Sender::s_block);
        }

        TLOG_DEBUG(5) << "Setting up SubscriberInfo objects";
        for (size_t group = 0; group < config.num_groups; ++group) {
            // subscribers.push_back(std::make_shared<SubscriberInfo>(group));
            for (size_t conn = 0; conn < config.num_connections_per_group;
                 ++conn) {
                subscribers.push_back(
                    std::make_shared<SubscriberInfo>(group, conn));
            }
        }

        std::atomic<std::chrono::steady_clock::time_point> last_received =
            std::chrono::steady_clock::now();
        TLOG_DEBUG(5) << "Adding callbacks for each subscriber";
        std::for_each(
            std::execution::par_unseq, std::begin(subscribers),
            std::end(subscribers),
            [=, &last_received](std::shared_ptr<SubscriberInfo> info) {
                auto recv_proc = [=, &last_received](
                                     dunedaq::datafilter::Data& msg) {
                    TLOG_DEBUG(3)
                        << "Received message " << msg.seq_number
                        << " with size " << msg.contents.size()
                        << " bytes from connection "
                        << config.get_connection_name(msg.publisher_id,
                                                      msg.group_id, msg.conn_id)
                        << " at " << info->get_connection_name(config);

                    TLOG() << "====> Trigger_number received: "
                           << msg.trigger_number << "\n";
                    TLOG() << "====> TR Dispatcher run number received: "
                           << msg.run_number << "\n";
                    TLOG() << "====> record_header_dataset: " << msg.path_header
                           << "\n";
                    TLOG() << "First 20 entries of a frame data received from "
                              "the TR Dispatcher of "
                           << msg.n_frames;

                    if (msg.contents.size() != config.message_size_kb * 1024 ||
                        msg.seq_number !=
                            info->last_sequence_received[msg.conn_id] + 1) {
                        info->msgs_with_error++;
                    }

                    // To check accepted condition here using the data filter
                    // algorithms.
                    TLOG() << "======>trwriter2";
                    organiser.accepted_trigger_record(
                        msg.trigger_number, msg.trigger_timestamp,
                        msg.run_number, msg.seq_number, msg.n_frames,
                        msg.element_id, msg.detector_id, msg.contents);

                    info->last_sequence_received[msg.conn_id] = msg.seq_number;
                    info->msgs_received++;
                    last_received = std::chrono::steady_clock::now();

                    if (info->msgs_received >= config.num_messages &&
                        !info->is_group_subscriber) {
                        TLOG_DEBUG(3) << "Complete condition reached, sending "
                                         "init message for "
                                      << info->get_connection_name(config);
                        // Handshake q(config.my_id, info->group_id,
                        // info->conn_id, run_number);
                        // init_sender->send(std::move(q), Sender::s_block);

                        info->complete = true;
                    }
                };

                auto before_receiver = std::chrono::steady_clock::now();
                auto receiver =
                    dunedaq::get_iom_receiver<dunedaq::datafilter::Data>(
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
                    //        "trdispatcher2");
                    "TR_tracking2");
            TLOG() << "send wait for next instruction";
            dunedaq::datafilter::Handshake q("wait");
            next_tr_sender->send(std::move(q), Sender::s_block);
        }

        // organiser.rewriter.send_trigger_record();
        organiser.rewriter.send_tr2();

        TLOG_DEBUG(5) << "Starting wait loop for receives to complete";
        bool all_done = false;
        while (!all_done) {
            size_t recvrs_done = 0;
            for (auto& sub : subscribers) {
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
        for (auto& info : subscribers) {
            auto receiver =
                dunedaq::get_iom_receiver<dunedaq::datafilter::Data>(
                    info->get_connection_name(config));
            receiver->remove_callback();
        }

        subscribers.clear();
        TLOG_DEBUG(5) << "receive() done";
    }
    void receive_tr(size_t run_number1) {
        bool handshake_done = false;
        std::atomic<unsigned int> received_cnt = 0;

        std::stringstream ss;

        ss << "datafilter sub: Preparing to receive Trigger Record";
        TLOG() << ss.str();
        ss.str("");

        auto cb_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
                "TR_tracking2");
        std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
            [&](dunedaq::datafilter::Handshake msg) {
                if (msg.msg_id == "next_tr") {
                    config.num_messages = msg.total_tr;
                    ++received_cnt;
                }
                TLOG_DEBUG(5)
                    << "datafilter: TR receiver callback: " << msg.msg_id;
            };

        cb_receiver->add_callback(str_receiver_cb);
        while (!handshake_done) {
            if (received_cnt == 1) handshake_done = true;
        }

        //        if (config.next_tr) {
        //            auto next_tr_sender =
        //                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
        //                    "TR_tracking2");
        //            TLOG() << "send next_tr instruction";
        //            dunedaq::datafilter::Handshake q("next_tr");
        //            next_tr_sender->send(std::move(q), Sender::s_block);
        //        }

        TLOG() << "datafilter sub: Setting up subscribers objects";
        for (size_t group = 0; group < config.num_groups; ++group) {
            for (size_t conn = 0; conn < config.num_connections_per_group;
                 ++conn) {
                subscribers.push_back(
                    std::make_shared<SubscriberInfo>(group, conn));
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
        TLOG() << "datatilter sub: adding callbacks for each subscriber";
        std::for_each(
            std::execution::par_unseq, std::begin(subscribers),
            std::end(subscribers),
            [=, &last_received](std::shared_ptr<SubscriberInfo> info) {
                auto recv_proc = [=, &last_received](trigger_record_ptr_t& tr) {
                    auto trigger_timestamp =
                        tr->get_fragments_ref().at(0)->get_trigger_timestamp();
                    auto trigger_number =
                        tr->get_fragments_ref().at(0)->get_trigger_number();
                    auto run_number =
                        tr->get_fragments_ref().at(0)->get_run_number();

                    auto frag_size = tr->get_fragments_ref().at(0)->get_size();
                    info->total_size_bytes = tr->get_total_size_bytes();
                    auto fg_window_begin =
                        tr->get_fragments_ref().at(0)->get_window_begin();
                    auto fg_window_end =
                        tr->get_fragments_ref().at(0)->get_window_end();

                    TLOG() << "trigger_timestamp " << trigger_timestamp
                           << " trigger_number " << trigger_number
                           << " run_number " << run_number << " fragment size "
                           << frag_size << " TR Total size bytes "
                           << info->total_size_bytes << " window_begin "
                           << fg_window_begin << " window_end "
                           << fg_window_end;

                    info->msgs_received++;
                    last_received = std::chrono::steady_clock::now();

                    organiser.accepted_trigger_record2(tr);

                    if (info->msgs_received == config.num_messages) {
                        TLOG() << "msgs_received from connection name:"
                               << info->get_connection_name(config);
                        std::string app_name = "test";
                        std::string ofile_name =
                            config.odir + "/" + config.output_h5_filename +
                            // std::to_string(info->msgs_received.load()) +
                            std::to_string(trigger_number) + ".hdf5";
                        //  create the file to write the TriggerRecords
                        /*
                    int file_index = 0;
                        std::unique_ptr<HDF5RawDataFile> h5file_ptr(
                            new HDF5RawDataFile(
                                ofile_name, run_number, file_index,
                                app_name, flp_json_in, srcid_geoid_map,
                                ".writing", HighFive::File::Overwrite));
                        h5file_ptr->write(*tr);
                        h5file_ptr.reset();
    */
                        // organiser.rewriter.send_tr(tr);
                        // organiser.accepted_trigger_record2(tr);

                        info->complete = true;
                    }
                };

                TLOG() << "Using connection: "
                       << info->get_connection_name(config);

                auto before_receiver = std::chrono::steady_clock::now();
                auto receiver = dunedaq::get_iom_receiver<trigger_record_ptr_t>(
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
                auto elapsed_time = info->get_receiver_time.count();
                if (elapsed_time > 0) {
                    auto transfer_rate = info->total_size_bytes / elapsed_time;
                    TLOG() << " Performance test: elapsed_time " << elapsed_time
                           << "transfer_rate " << transfer_rate;
                }
                // << "transfer rate" << rate;
            });

        //        if (config.next_tr) {
        //            auto next_tr_sender =
        //                dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
        //                    "TR_tracking2");
        //            TLOG() << "send wait for next instruction";
        //            dunedaq::datafilter::Handshake q("wait");
        //            next_tr_sender->send(std::move(q), Sender::s_block);
        //        }

        TLOG_DEBUG(5) << "Starting wait loop for receives to complete";
        bool all_done = false;
        while (!all_done) {
            size_t recvrs_done = 0;
            for (auto& sub : subscribers) {
                if (sub->complete.load()) recvrs_done++;
            }
            TLOG_DEBUG(7) << "Done: " << recvrs_done << ", expected: "
                          << config.num_groups *
                                 config.num_connections_per_group;
            all_done = recvrs_done >=
                       config.num_groups * config.num_connections_per_group;
            if (!all_done) std::this_thread::sleep_for(1ms);
        }
        TLOG() << "Removing callbacks";
        for (auto& info : subscribers) {
            auto receiver = dunedaq::get_iom_receiver<trigger_record_ptr_t>(
                info->get_connection_name(config));
            receiver->remove_callback();
        }

        subscribers.clear();
        TLOG() << "receive() done";
    }
};
}  // namespace datafilter
// Must be in dunedaq namespace only
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Data, "data_t");
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Handshake, "init_t");
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::BookKeeping, "bk_t");
// DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::BookKeeping_json, "bk_t");
}  // namespace dunedaq

int main(int argc, char* argv[]) {
    dunedaq::logging::Logging::setup();
    dunedaq::datafilter::DataFilterConfig config;

    bool help_requested = false;
    namespace po = boost::program_options;
    po::options_description desc(
        "Program to test IOManager load with many connections");
    desc.add_options()("use_connectivity_service,c",
                       po::bool_switch(&config.use_connectivity_service),
                       "enable the ConnectivityService in IOManager")(
        "next_tr,x",
        po::value<bool>(&config.next_tr)->default_value(config.next_tr),
        "get next TR")(
        "num_apps,N",
        po::value<size_t>(&config.num_apps)->default_value(config.num_apps),
        "Number of applications to start")(
        "num_groups,g",
        po::value<size_t>(&config.num_groups)->default_value(config.num_groups),
        "Number of connection groups")(
        "num_connections,n",
        po::value<size_t>(&config.num_connections_per_group)
            ->default_value(config.num_connections_per_group),
        "Number of connections to register and use in each group")(
        "port,p", po::value<int>(&config.port)->default_value(config.port),
        "port to connect to on configuration server")(
        "server,s",
        po::value<std::string>(&config.server)->default_value(config.server),
        "datafilter server")("server_trdispatcher,st",
                             po::value<std::string>(&config.server_trdispatcher)
                                 ->default_value(config.server_trdispatcher),
                             "trdispatcher server")(
        "num_messages,m",
        po::value<size_t>(&config.num_messages)
            ->default_value(config.num_messages),
        "Number of messages to send on each connection")(
        "message_size_kb,z",
        po::value<size_t>(&config.message_size_kb)
            ->default_value(config.message_size_kb),
        "Size of each message, in KB")(
        "num_runs,r",
        po::value<size_t>(&config.num_runs)->default_value(config.num_runs),
        "Number of times to clear the sender and send all messages")(
        "publish_interval,i",
        po::value<int>(&config.publish_interval)
            ->default_value(config.publish_interval),
        "Interval, in ms, for ConfigClient to re-publish connection info")(
        "send_interval,I",
        po::value<size_t>(&config.send_interval_ms)
            ->default_value(config.send_interval_ms),
        "Interval, in ms, for Publishers to send messages")(
        "output_file_base,o",
        po::value<std::string>(&config.info_file_base)
            ->default_value(config.info_file_base),
        "Base name for output info file (will have _sender.csv or "
        "_receiver.csv appended)")("session",
                                   po::value<std::string>(&config.session_name)
                                       ->default_value(config.session_name),
                                   "Name of this DAQ session")(
        "help,h", po::bool_switch(&help_requested), "Print this help message");

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

    if (config.use_connectivity_service) {
        TLOG() << "Setting Connectivity Service Environment variables";
        config.configure_connsvc();
    }

    auto startup_time = std::chrono::steady_clock::now();
    // start fork process : we don't need it for now
    //   std::vector<pid_t> forked_pids;
    //   for (size_t ii = 0; ii < config.num_apps; ++ii) {
    //     auto pid = fork();
    //     if (pid < 0) {
    //        TLOG() <<"fork error";
    //        exit(EXIT_FAILURE);
    //     } else if (pid == 0) { // child
    //
    //       forked_pids.clear();
    //       config.my_id = ii;
    //
    //       TLOG() << "DataFilter : child process " << config.my_id
    //       <<"ii="<<ii; break;
    //     } else {
    //         TLOG() << "DataFilter : parent process " << getpid();
    //         forked_pids.push_back(pid);
    //     }
    //   }

    //    std::this_thread::sleep_until(startup_time + 2s);

    TLOG() << "DataFilter" << config.my_id1 << ": "
           << "Configuring IOManager for receiving TriggerRecords";
    TLOG() << "DataFilter" << config.my_id2 << ": "
           << "Configuring IOManager for sending TriggerRecords";
    config.configure_iomanager();

    auto subscriber =
        std::make_unique<dunedaq::datafilter::SubscriberTest>(config);
    auto trrewriter = std::make_unique<dunedaq::datafilter::TRRewriter>(config);

    // Create a thread for receive_bk
    std::thread bk_thread(&dunedaq::datafilter::SubscriberTest::receive_bk,
                          subscriber.get());

    for (size_t run = 0; run < config.num_runs; ++run) {
        TLOG() << "Subscriber " << config.my_id1 << ": "
               << "Starting test run " << run;
        if (config.num_apps > 1) {
            subscriber->init(run);
            trrewriter->init(run);
        }
        subscriber->organiser.send_next_tr();
        subscriber->receive_tr(run);

        // subscriber->subscribers.pop_back();

        TLOG() << "Subscriber " << config.my_id1 << ": "
               << "Test run " << run << " complete.";
    }

    TLOG() << "Subscriber " << config.my_id1 << ": "
           << "Cleaning up";

    // Wait for the receive_bk thread to finish
    if (bk_thread.joinable()) {
        bk_thread.join();
    }
    subscriber.reset(nullptr);

    dunedaq::iomanager::IOManager::get()->reset();
    TLOG() << "Subscriber " << config.my_id1 << ": "
           << "DONE";

    //  if (forked_pids.size() > 0) {
    //    TLOG() << "Waiting for forked PIDs";
    //
    //    for (auto& pid : forked_pids) {
    //      siginfo_t status;
    //      auto sts = waitid(P_PID, pid, &status, WEXITED);
    //
    //      TLOG_DEBUG(6) << "Forked process " << pid << " exited with status "
    //      << status.si_status << " (wait status " << sts
    //                    << ")";
    //    }
    //  }
};
