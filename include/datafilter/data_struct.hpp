#include <utility>  // for std::pair
#include <variant>
#include <vector>

#include "serialization/Serialization.hpp"

namespace dunedaq {
namespace datafilter {

// using bk_info_ =
//     std::map<std::string,
//              std::variant<std::string, double, std::vector<double>>>;
//
using VariantType = std::variant<std::string, double, std::vector<double>>;

struct Data {
    size_t seq_number;
    size_t trigger_number;
    size_t trigger_timestamp;
    size_t run_number;
    size_t element_id;
    size_t detector_id;
    size_t error_bits;
    size_t fragment_type;
    // daqdataformats::Fragment  fragment_type;
    std::string path_header;
    int n_frames;

    size_t publisher_id;
    size_t group_id;
    size_t conn_id;

    std::vector<int> contents;

    Data() = default;
    Data(size_t seq, size_t trigger, size_t timestamp, size_t run,
         size_t element, size_t detector, size_t error, size_t fragment,
         std::string path, int nframes, size_t publisher, size_t group,
         size_t conn, size_t size)
        : seq_number(seq),
          trigger_number(trigger),
          trigger_timestamp(timestamp),
          run_number(run),
          element_id(element),
          detector_id(detector),
          error_bits(error),
          fragment_type(fragment),
          path_header(path),
          n_frames(nframes),
          publisher_id(publisher),
          group_id(group),
          conn_id(conn),
          contents(size) {}
    virtual ~Data() = default;
    Data(Data const&) = default;
    Data& operator=(Data const&) = default;
    Data(Data&&) = default;
    Data& operator=(Data&&) = default;

    DUNE_DAQ_SERIALIZE(Data, seq_number, trigger_number, trigger_timestamp,
                       run_number, element_id, detector_id, error_bits,
                       fragment_type, path_header, n_frames, publisher_id,
                       group_id, conn_id, contents);
};

// struct Payload
//{
//     std::unique_ptr<daqdataformats::trigger_records> tr;
//     Payload() = default;
//     Payload()
//     DUNE_DAQ_SERIALIZE(Payload,tr);
//
// }

struct BookKeeping {
    std::string entry_id;
    std::string conn_id;
    std::string from_id;
    std::string data_filter_id;
    std::vector<std::pair<std::string, std::string>> node{};
    std::vector<std::pair<std::string, std::string>> tr_header_info{};
    std::string tr_status{};

    std::vector<std::string> file_send_list{};
    // std::string file_send_fail_list{};
    std::string file_send_status{};  // sended or receive, or transit.
    size_t transfer_rate;
    std::string write_status{};
    unsigned int run_number;
    BookKeeping() = default;
    BookKeeping(std::string entry) : entry_id(entry) {}
    DUNE_DAQ_SERIALIZE(BookKeeping, entry_id, conn_id, from_id, data_filter_id,
                       node, tr_header_info, tr_status, file_send_list,
                       file_send_status, transfer_rate, write_status,
                       run_number);
};

// struct BookKeeping_json {
//     std::string msg_id;
//     std::map<std::string, std::variant<std::string>> bk_info;
//     BookKeeping_json() = default;
//     BookKeeping_json(std::string msg) : msg_id(msg){};
//     DUNE_DAQ_SERIALIZE(BookKeeping_json, bk_info);
// };

// struct BookKeeping_json {
//     std::string msg_id;
//     nlohmann::json bk_info;
//     BookKeeping_json() = default;
//     BookKeeping_json(std::string msg) : msg_id(msg){};
//     DUNE_DAQ_SERIALIZE(BookKeeping_json, bk_info);
// };
struct Handshake {
    std::string msg_id;
    int total_tr;
    Handshake() = default;
    Handshake(std::string msg) : msg_id(msg) {}

    DUNE_DAQ_SERIALIZE(Handshake, msg_id, total_tr);
};

}  // namespace datafilter
}  // namespace dunedaq
