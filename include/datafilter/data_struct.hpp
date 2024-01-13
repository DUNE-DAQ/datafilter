#include "serialization/Serialization.hpp"

namespace dunedaq {
namespace datafilter {

struct Data
{
  size_t seq_number;
  size_t trigger_number;
  size_t trigger_timestamp;
  size_t run_number;
  size_t element_id;
  size_t detector_id;
  size_t error_bits;
  size_t fragment_type;
  //daqdataformats::Fragment  fragment_type;
  std::string path_header;
  int n_frames;

  size_t publisher_id;
  size_t group_id;
  size_t conn_id;
  
  //std::vector<uint8_t> contents;
  std::vector<int> contents;

  Data() = default;
  Data(size_t seq, size_t trigger, size_t timestamp, size_t run, 
          size_t element, size_t detector, size_t error, size_t fragment, std::string path, int nframes, 
          size_t publisher, size_t group, size_t conn, size_t size)
    : seq_number(seq)
    , trigger_number(trigger)
    , trigger_timestamp(timestamp)
    , run_number(run)
    , element_id(element)
    , detector_id(detector)
    , error_bits(error)
    , fragment_type(fragment)
    , path_header(path)
    , n_frames(nframes)
    , publisher_id(publisher)
    , group_id(group)
    , conn_id(conn)
    , contents(size)
  {
  }
  virtual ~Data() = default;
  Data(Data const&) = default;
  Data& operator=(Data const&) = default;
  Data(Data&&) = default;
  Data& operator=(Data&&) = default;

  DUNE_DAQ_SERIALIZE(Data, seq_number, trigger_number, trigger_timestamp,
         run_number, element_id, detector_id, error_bits, fragment_type, path_header, n_frames, 
         publisher_id, group_id, conn_id, contents);
};

struct Handshake
{
  std::string msg_id;
  Handshake() = default;
  Handshake(std::string msg)
      : msg_id(msg)
  {
  }

  DUNE_DAQ_SERIALIZE(Handshake, msg_id);

};


struct QuotaReached
{
  size_t sender_id;
  int group_id;
  size_t conn_id;
  size_t run_number;

  QuotaReached() = default;
  QuotaReached(size_t sender, int group, size_t conn, size_t run)
    : sender_id(sender)
    , group_id(group)
    , conn_id(conn)
    , run_number(run)
  {
  }

  DUNE_DAQ_SERIALIZE(QuotaReached, sender_id, group_id, conn_id, run_number);
};

}
}
