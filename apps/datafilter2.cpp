#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/hdf5filelayout/Nljs.hpp"
#include "hdf5libs/hdf5rawdatafile/Nljs.hpp"

#include "detdataformats/DetID.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"


#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

#include "datafilter/data_struct.hpp"

#include "boost/program_options.hpp"

#include <algorithm>
#include <execution>
#include <fstream>
#include <sys/wait.h>

using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::detdataformats;


namespace dunedaq {
namespace iomanager {

struct DatafilterConfig
{

  bool use_connectivity_service = false; 
  int port = 5000;
  std::string server = "localhost";
  std::string info_file_base = "datafilter";
  std::string session_name = "iomanager : datafilter";
  size_t num_apps = 1;
  size_t num_connections_per_group = 1;
  size_t num_groups = 1;
  size_t num_messages = 1;
  size_t message_size_kb = 1024;
  size_t num_runs = 1;
  size_t my_id = 0;
  size_t send_interval_ms = 100;
  int publish_interval = 1000;
  bool next_tr=false;

  size_t seq_number;
  size_t trigger_number;
  size_t trigger_timestamp;
  size_t run_number;
  size_t element_id;
  size_t detector_id;
  size_t error_bits;
  size_t fragment_type;
  //dunedaq::daqdataformats::FragmentType fragment_type;


  std::string input_h5_filename = "/lcg/storage19/test-area/dune/trigger_records/swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";
  std::string output_h5_filename = "/opt/tmp/chen/h5_test.hdf5";

  void configure_connsvc()
  {
    setenv("CONNECTION_SERVER", server.c_str(), 1);
    setenv("CONNECTION_PORT", std::to_string(port).c_str(), 1);
  }

  std::string get_connection_name(size_t app_id, size_t group_id, size_t conn_id)
  {
    std::stringstream ss;
    ss << "conn_A" << app_id << "_G" << group_id << "_C" << conn_id << "_";
    return ss.str();
  }
  std::string get_group_connection_name(size_t app_id, size_t group_id)
  {

    std::stringstream ss;
    ss << "conn_A" << app_id << "_G" << group_id << "_.*";
    return ss.str();
  }

  std::string get_connection_ip(size_t app_id, size_t group_id, size_t conn_id)
  {
    assert(num_apps < 253);
    assert(num_groups < 253);
    assert(num_connections_per_group < 252);

    int first_byte = conn_id + 2;   // 2-254
    int second_byte = group_id + 1; // 1-254
    int third_byte = app_id + 1;    // 1 - 254

    std::string conn_addr = "tcp://127." + std::to_string(third_byte) + "." + std::to_string(second_byte) + "." +
                            std::to_string(first_byte) + ":15500";

    return conn_addr;
  }

  std::string get_subscriber_init_name() { return get_subscriber_init_name(my_id); }
  std::string get_subscriber_init_name(size_t id) { return "conn_init_" + std::to_string(id); }
  //std::string get_publisher_init_name() { return "conn_init_.*"; }


  void configure_iomanager()
  {
    setenv("DUNEDAQ_PARTITION", session_name.c_str(), 0);

    Queues_t queues;
    Connections_t connections;

      for (size_t group = 0; group < num_groups; ++group) {
        for (size_t conn = 0; conn < num_connections_per_group; ++conn) {
          auto conn_addr = get_connection_ip(my_id, group, conn);
          TLOG() << "Adding connection with id " << get_connection_name(my_id, group, conn) << " and address "
                        << conn_addr;

          connections.emplace_back(Connection{
            ConnectionId{ get_connection_name(my_id, group, conn), "data_t" }, conn_addr, ConnectionType::kPubSub });
        }
      }

    //  for (size_t sub = 0; sub < num_apps; ++sub) {
      for (size_t sub = 0; sub < 3; ++sub) {
        auto port = 13000 + sub;
        std::string conn_addr = "tcp://127.0.0.1:" + std::to_string(port);
        TLOG() << "Adding control connection " << "TR_tracking"+std::to_string(sub) << " with address "
                      << conn_addr;

        connections.emplace_back(
          //Connection{ ConnectionId{ "TR_tracking"+std::to_string(sub), "init_t" }, conn_addr, ConnectionType::kPubSub });
          Connection{ ConnectionId{ "TR_tracking"+std::to_string(sub), "init_t" }, conn_addr, ConnectionType::kSendRecv });
      }


    IOManager::get()->configure(
      queues, connections, use_connectivity_service, std::chrono::milliseconds(publish_interval));
  }
};
struct SubscriberTest
{
  struct SubscriberInfo
  {
    size_t group_id;
    size_t conn_id;
    bool is_group_subscriber;
    std::unordered_map<size_t, size_t> last_sequence_received{ 0 };
    std::atomic<size_t> msgs_received{ 0 };
    std::atomic<size_t> msgs_with_error{ 0 };
    std::chrono::milliseconds get_receiver_time;
    std::chrono::milliseconds add_callback_time;
    std::atomic<bool> complete{ false };

    SubscriberInfo(size_t group, size_t conn)
      : group_id(group)
      , conn_id(conn)
      , is_group_subscriber(false)
    {
    }
    SubscriberInfo(size_t group)
      : group_id(group)
      , conn_id(0)
      , is_group_subscriber(true)
    {
    }

    std::string get_connection_name(DatafilterConfig& config)
    {
      if (is_group_subscriber) {
        return config.get_group_connection_name(config.my_id, group_id);
      }
      return config.get_connection_name(config.my_id, group_id, conn_id);
    }
  };
  std::vector<std::shared_ptr<SubscriberInfo>> subscribers;
  DatafilterConfig config;

  // create TriggerRecordHeader
  TriggerRecordHeaderData trh_data;

  explicit SubscriberTest(DatafilterConfig c)
    : config(c)
  {
  }
    uint16_t data3[200000000];
    uint32_t nchannels=64;
    uint32_t nsamples=64;

    std::string path_header1;

  void init(size_t datafilter_run_number){

    TLOG_DEBUG(5) << "Getting init sender";
    auto init_receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("TR_tracking0");
    std::atomic<std::chrono::steady_clock::time_point> last_received = std::chrono::steady_clock::now();
    while (
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_received.load())
        .count() < 500) {
      //Handshake q(config.my_id, -1, 0, run_number);
      TLOG()<<"datafilter2";
      dunedaq::datafilter::Handshake recv;
      recv=init_receiver->receive(std::chrono::milliseconds(100));
      TLOG()<<"message received:  "<<recv.msg_id;
      if (recv.msg_id=="start")
            break;
      std::this_thread::sleep_for(100ms);
    }

    auto init_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("TR_tracking1");
    std::atomic<std::chrono::steady_clock::time_point> last_received1 = std::chrono::steady_clock::now();
    while (
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_received1.load())
           .count() < 500) {
         dunedaq::datafilter::Handshake q("gotit");
         init_sender->send(std::move(q), Sender::s_block);
         std::this_thread::sleep_for(100ms);
      }

  }
//    int get_data()
//    {
//          //hdf5libs here
//          auto cnt=0;  // "/" counter 
//          bool is_replace = 0;
//        
//          //open h5 file
//          HDF5RawDataFile h5_file(config.input_h5_filename);
//          
//          std::ostringstream ss;
//          
//          ss << "\nFile name: " << h5_file.get_file_name();
//          ss << "\n\tRecorded size from class: " << h5_file.get_recorded_size();
//          
//          auto recorded_size = h5_file.get_attribute<size_t>("recorded_size");
//          ss << "\n\tRecorded size from attribute: " << recorded_size;
//          
//          auto record_type = h5_file.get_record_type();
//          ss << "\nRecord type = " << record_type;
//          
//          nlohmann::json flp_json;
//          auto flp = h5_file.get_file_layout().get_file_layout_params();
//        
//          hdf5filelayout::to_json(flp_json, flp);
//          ss << "\nFile Layout Parameters:\n" << flp_json;
//          
//          // get some attributs from h5_file
//          auto run_number = h5_file.get_attribute<unsigned int>("run_number");
//          auto file_index = h5_file.get_attribute<unsigned int>("file_index");
//          auto creation_timestamp = h5_file.get_attribute<std::string>("creation_timestamp");
//          auto app_name = h5_file.get_attribute<std::string>("application_name");
//          //auto all_trigger_record_numbers = h5_file.get_all_trigger_record_numbers();
//        
//          ss << "\n Run number = " << run_number;
//          ss << "\n File index: " << file_index;
//          ss << "\n Application name: " << app_name;
//          //ss << "\n all_trigger_record_numbers"<<all_trigger_record_numbers;
//        
//          //TLOG() << ss.str();
//          ss.str("");
//       
//          const int trigger_count = recorded_size;
//          auto records = h5_file.get_all_record_ids();
//          ss << "\nNumber of records: " << records.size();
//          if (records.empty()) {
//          	ss << "\n\nNO TRIGGER RECORDS FOUND";
//          	TLOG() << ss.str();
//          	return 0;
//          }
//          auto first_rec = *(records.begin());
//          auto last_rec = *(std::next(records.begin(), records.size() - 1));
//        
//          ss << "\n\tFirst record: " << first_rec.first << "," << first_rec.second;
//          ss << "\n\tLast record: " << last_rec.first << "," << last_rec.second;
//          
//          TLOG() << ss.str();
//          ss.str("");
//        
//          auto all_rh_paths = h5_file.get_record_header_dataset_paths();
//          ss << "\nAll record header datasets found:";
//          for (auto const& path : all_rh_paths)
//          	ss << "\n\t" << path;
//          TLOG() << ss.str();
//          ss.str("");
//        
//          if(h5_file.is_trigger_record_type()) {
//          	auto trh_ptr = h5_file.get_trh_ptr(first_rec);
//          	ss << "\nTrigger Record Headers:";
//          	ss << "\nFirst: " << trh_ptr->get_header();
//          	ss << "\nLast: " << h5_file.get_trh_ptr(all_rh_paths.back())->get_header();
//          } else if(h5_file.is_timeslice_type()) {
//          	auto tsh_ptr = h5_file.get_tsh_ptr(first_rec);
//          	ss << "\nTimeSlice Headers:";
//          	ss << "\nFirst: " << *tsh_ptr;
//          	ss << "\nLast: " << *(h5_file.get_tsh_ptr(all_rh_paths.back()));
//          }
//          TLOG() << ss.str();
//          ss.str("");
//                                                                                         //
//          for (auto const& rid : records) {
//        		auto record_header_dataset = h5_file.get_record_header_dataset_path(rid);
//              auto tr = h5_file.get_trigger_record(rid);
//      		auto trh_ptr = h5_file.get_trh_ptr(rid);
//
//            path_header1 = record_header_dataset;
//
//           	ss << "\n trh_ptr \t" << trh_ptr->get_header();
//              ss << "\n record_header_dataset \t"<<record_header_dataset;
//              // get a timestamp for this trigger
//              uint64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>( // NOLINT(build/unsigned)
//                            system_clock::now().time_since_epoch())
//                            .count();
//        
//              auto trig_num = trh_ptr->get_trigger_number();
//              auto num_requested_components = trh_ptr->get_num_requested_components();
//              ss <<"\n num_requested_components \t"<<num_requested_components;
//              auto seq_num = trh_ptr->get_sequence_number();
//              auto max_seq_num = trh_ptr->get_max_sequence_number();
//              ss <<"\n seq_num \t"<<seq_num;
//              config.seq_number=seq_num;
//              config.trigger_number=trig_num;
//
//          	  //TLOG() << ss.str(); ss.str("");
//        
//        
//              //std::vector<uint64_t> data2 = frag_ptr->get_data();
//              std::vector<uint16_t > data2;
//
//              auto frag_paths =h5_file.get_fragment_dataset_paths(rid);
//              auto all_frag_paths = h5_file.get_all_fragment_dataset_paths();
//              for (auto const& path : frag_paths ) {
//                  auto frag_ptr = h5_file.get_frag_ptr(path);
//                  auto fragment_size=frag_ptr->get_size();
//                  auto fragment_type = frag_ptr->get_fragment_type();
//                  
//                  auto elem_id = frag_ptr->get_element_id();
//
//                  //config.element_id=elem_id;
//                  auto data = frag_ptr->get_data();
//
//                  //std::vector<uint64_t*>& data1 = *reinterpret_cast<std::vector<uint64_t*> *>(data);
//
//                  int nframes = (fragment_size-sizeof(daqdataformats::FragmentHeader))/sizeof(fddetdataformats::WIBEthFrame);
//              //    py::array_t<uint16_t> ret(256*nframes);
//              //    auto data2 = static_cast<uint16_t*>(ret.request().data2);
//              //    auto data2 = std::make_unique<uint16_t>();
//
//                  //std::cout<<"fragment_size"<<fragment_size<<" "<<nframes<<"\n";
//                  for (auto i=0;i<nframes;++i){
//                      auto fr = reinterpret_cast<fddetdataformats::WIBEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::WIBEthFrame));
//                      for (auto j=0;j<nsamples;++j){
//                          for (auto k=0;k<nchannels;++k){
//                               data3[(nsamples*nchannels)*i + nchannels*j + k]=fr->get_adc(k,j);
//                               //std::cout<<"=======================>"<<i<<" "<<j<<" "<<data3[nsamples*nchannels*i + nchannels*j+k]<<'\n';
//                          }
//                      }
//                  //for (auto &i: data) {
//                  //for (auto i=0;data1.size();i++)
//                     // std::cout<<"=======================>"<<"i"<<"  "<<data1[i]<<"\n";
//                    //std::cout<<"=======================>"<<i<<" "<<((uint64_t *)data)[i]<<'\n';
//                    //data1[i]=((uint64_t *)data)[i];
//                  //  std::cout<<"============="<<i<<'\n';
//                  }
//
//                  //for (auto i=0;data2.size();i++){
//                  //    std::cout<<"========>"<<data2[i]<<"\n";
//                  //} 
////                  std::vector<std::unique_ptr<Fragment>> frag_ptr1; 
////        
////                  if ( config.write_fragment_type==0 )
////                     std::vector<char> dummy_data1(fragment_size-80 ,0);
////                  else
////                      fragment_size=80;
////                     std::vector<char> dummy_data1(fragment_size ,0);
////        
////                  for (auto& i: dummy_data1 ) {
////                      i=1;
////                      
////                  }
////      
////                  //TLOG()<<"<====> path "<<path;
////        
////                  std::istringstream isSS(path);
////                  std::string token;
////        
////                  cnt=0;
////                  while (std::getline(isSS,token,'/')) {
////                      if (!token.empty()) {
////                          cnt++;
////                          if (config.write_fragment_type==0 && cnt==4) {
////                              if (token=="Link02") {
////                                  is_replace=1;
////                              } else {
////                                  //TLOG()<<"<<==>>token "<<token<<" path "<<path;
////                                  is_replace=0;
////                              } 
////                          }
////                          if (config.write_fragment_type==1 && cnt==4) {                    
////                              if (token == "Element00001") {
////                                  TLOG()<<"<<==>>"<<token<<" will be removed from path "<<path;
////                                  is_replace=1;
////                              } else {
////                                  is_replace=0;
////                              }
////                          }
////                        }
////                  }
////        
////                    if (is_replace==1) {
////                        //std::vector<std::unique_ptr<Fragment>> frag_ptr1; 
////                        //std::vector<std::pair<void*, size_t>> list_of_pieces(fragment_size,std::make_pair(void*,0));
////                        //std::vector<std::pair<void*, size_t>> list_of_pieces;
////                        //for (auto& list_of_piece : list_of_pieces)
////                        //    TLOG()<<"\n list_of_piece \t"<<list_of_piece.first;
////       
////                        // create our fragment
////                        FragmentHeader fh;
////                        fh.trigger_number = trig_num;
////                        fh.trigger_timestamp = ts;
////                        fh.window_begin = ts - 10;
////                        fh.window_end = ts;
////                        fh.run_number = run_number;
////                        fh.fragment_type = int(fragment_type);
////                        fh.sequence_number = seq_num;
////                        //fh.element_id = GeoID(gtype_to_use, reg_num, ele_num);
////                        fh.element_id = elem_id;
////        
////                        //std::unique_ptr<Fragment> frag(new Fragment(list_of_pieces));
////                       
////                        // this is another way to set the fragment header
////                        //frag->set_type(frag_ptr->get_fragment_type());
////                        //frag->set_detector_id(frag_ptr->get_detector_id());
////                        //frag->set_run_number(run_number);
////                        //frag->set_trigger_number(trig_num);
////                        //frag->set_window_begin(ts-10);
////                        //frag->set_window_end(ts);
////                        //frag->set_element_id(elem_id);
////                        //frag->set_type(daqdataformats::FragmentType::kTriggerPrimitives);
////                        //frag->set_header_fields(frag_ptr->get_header());
////                        //
////                        //frag_ptr1.push_back(std::move(frag));
////                        //  //frag_ptr1->set_header_fields(frag_ptr->get_header());
////                        // 
////                        //  //int num_frames = fragment_size/sizeof(detdataformats::wib::WIBFrame);
////        
////                        auto frag_ptr2 = std::make_unique<Fragment>(dummy_data1.data(), dummy_data1.size());
////                        //frag_ptr2->set_header_fields(fh);
////                        frag_ptr2->set_header_fields(frag_ptr->get_header());
////       
////                        //auto record_number = h5_file.get_file_layout().get_record_number_string(trig_num,seq_num);
////                        //std::cout<<"\n =====record_number_string \t"<<record_number; 
////                        //frag_ptr2->set_header_fields(frag->get_header());
////                        if (config.write_fragment_type==0) {
////                            TLOG()<<"Link02 found in path and will be replaced by a vector with 1 in " << path;
////                            tr1.add_fragment(std::move(frag_ptr2));
////                        }
////              } else {
////                     
////                        tr1.add_fragment(std::move(frag_ptr));
////              }
//        
//              //dummy_data1.clear();
//            }   // end loop over regions
//        
//        
//            // write trigger record to file
//            //if (trig_num%10!=0) {
//      //      h5_raw_data_file.write(tr1);
//          } // end loop over triggers
//        
//          //TLOG() << "Finished writing to file " << h5_raw_data_file.get_file_name();
//          //TLOG() << "Recorded size: " << h5_raw_data_file.get_recorded_size();
//       
//        //end hdf5libs
//    
//    return 0; 
//    }
//    

  void receive(size_t run_number1)
  {

      if (config.next_tr) {
          auto next_tr_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("TR_tracking2");
          TLOG()<<"send next_tr instruction";
          dunedaq::datafilter::Handshake q("next_tr");
          next_tr_sender->send(std::move(q), Sender::s_block);
      }

    TLOG_DEBUG(5) << "Setting up SubscriberInfo objects";
    for (size_t group = 0; group < config.num_groups; ++group) {
     // subscribers.push_back(std::make_shared<SubscriberInfo>(group));
      for (size_t conn = 0; conn < config.num_connections_per_group; ++conn) {
        subscribers.push_back(std::make_shared<SubscriberInfo>(group, conn));
      }
    }

    std::atomic<std::chrono::steady_clock::time_point> last_received = std::chrono::steady_clock::now();
    TLOG_DEBUG(5) << "Adding callbacks for each subscriber";
    std::for_each(std::execution::par_unseq,
                  std::begin(subscribers),
                  std::end(subscribers),
                  [=, &last_received](std::shared_ptr<SubscriberInfo> info) {
                    auto recv_proc = [=, &last_received](dunedaq::datafilter::Data& msg) {
                      TLOG_DEBUG(3) << "Received message " << msg.seq_number << " with size " << msg.contents.size()
                                    << " bytes from connection "
                                    << config.get_connection_name(msg.publisher_id, msg.group_id, msg.conn_id) << " at "
                                    << info->get_connection_name(config);

                    TLOG() <<"====> Trigger_number received: "<<msg.trigger_number<<"\n";
                    TLOG() <<"====> Dataflow emu run number received: "<<msg.run_number<<"\n";
                    TLOG() <<"====> record_header_dataset: " <<msg.path_header<<"\n";
                    TLOG() <<"First 20 entries of a frame data received from the dataflow emulator of "<<msg.n_frames;
//                    trh_data.trigger_number = msg.trigger_number;
//                    trh_data.trigger_timestamp = msg.trigger_timestamp;
//                    //trh_data.num_requested_components = num_requested_components;
//                    trh_data.run_number = msg.run_number;
//                    trh_data.sequence_number = msg.seq_number;
//                    //trh_data.max_sequence_number = max_seq_num;
//                  
//                    TriggerRecordHeader trh1(&trh_data);
//                    // create out TriggerRecord
//                    TriggerRecord tr1(trh1);
//                    // create our fragment
////                    FragmentHeader fh;
////                    fh.trigger_number = msg.trigger_number;
////                    fh.trigger_timestamp = msg.trigger_timestamp;
////                    fh.window_begin = msg.trigger_timestamp - 10;
////                    fh.window_end = msg.trigger_timestamp;
////                    fh.run_number = msg.run_number;
////                    fh.fragment_type = msg.fragment_type;
////                    fh.sequence_number = msg.seq_number;
////                    //fh.element_id = GeoID(gtype_to_use, reg_num, ele_num);
////                    //fh.element_id = elem_id;
////                    
//                    std::vector<std::pair<void*, size_t>> list_of_pieces;
//                    std::unique_ptr<Fragment> frag(new Fragment(list_of_pieces));
//                   
//                    // this is another way to set the fragment header
//                   // frag->set_type(msg.fragment_type);
//                    //frag->set_detector_id(frag_ptr->get_detector_id());
//                    frag->set_run_number(msg.run_number);
//                    frag->set_trigger_number(msg.trigger_number);
//                    frag->set_window_begin(msg.trigger_timestamp-10);
//                    frag->set_window_end(msg.trigger_timestamp);
//                    //frag->set_element_id(elem_id);
//                    //frag->set_type(daqdataformats::FragmentType::kTriggerPrimitives);
//
//                    auto data = frag->get_data();
                    
                    for (auto i = 0; i < msg.n_frames; ++i)
                    {
//                      auto fr = reinterpret_cast<fddetdataformats::WIBEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::WIBEthFrame));
                       if (i<20) 
                          TLOG() <<"Receiver: contents "<<msg.contents[i];
//                       for (auto j=0;j<nchannels;++j) {
//                           fr->set_adc(j,nsamples,msg.contents[i]);
//                       }

//                       tr1.add_fragment(std::move(frag));
                    }
                      if (msg.contents.size() != config.message_size_kb * 1024 ||
                          msg.seq_number != info->last_sequence_received[msg.conn_id] + 1) {
                        info->msgs_with_error++;
                      }
                      info->last_sequence_received[msg.conn_id] = msg.seq_number;
                      info->msgs_received++;
                      last_received = std::chrono::steady_clock::now();

                      if (info->msgs_received >= config.num_messages && !info->is_group_subscriber) {
                        TLOG_DEBUG(3) << "Complete condition reached, sending init message for "
                                      << info->get_connection_name(config);
                        //Handshake q(config.my_id, info->group_id, info->conn_id, run_number);
                        //init_sender->send(std::move(q), Sender::s_block);
                        info->complete = true;
                      }
                    };

                    auto before_receiver = std::chrono::steady_clock::now();
                    auto receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Data>(info->get_connection_name(config));
                    auto after_receiver = std::chrono::steady_clock::now();
                    receiver->add_callback(recv_proc);
                    auto after_callback = std::chrono::steady_clock::now();
                    info->get_receiver_time =
                      std::chrono::duration_cast<std::chrono::milliseconds>(after_receiver - before_receiver);
                    info->add_callback_time =
                      std::chrono::duration_cast<std::chrono::milliseconds>(after_callback - after_receiver);
                  });

     if (config.next_tr) {
          auto next_tr_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("TR_tracking2");
          TLOG()<<"send wait for next instruction";
          dunedaq::datafilter::Handshake q("wait");
          next_tr_sender->send(std::move(q), Sender::s_block);
      }

    TLOG_DEBUG(5) << "Starting wait loop for receives to complete";
    bool all_done = false;
    while (!all_done) {
      size_t recvrs_done = 0;
      for (auto& sub : subscribers) {
        if (sub->complete.load())
          recvrs_done++;
      }
      TLOG_DEBUG(6) << "Done: " << recvrs_done
                    << ", expected: " << config.num_groups * config.num_connections_per_group;
      all_done = recvrs_done >= config.num_groups * config.num_connections_per_group;
      if (!all_done)
        std::this_thread::sleep_for(1ms);
    }
    TLOG_DEBUG(5) << "Removing callbacks";
    for (auto& info : subscribers) {
      auto receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Data>(info->get_connection_name(config));
      receiver->remove_callback();
    }

    subscribers.clear();
    TLOG_DEBUG(5) << "receive() done";
  }
};
}
// Must be in dunedaq namespace only
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Data, "data_t");
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Handshake, "init_t");
}

int
main(int argc, char* argv[])
{
  dunedaq::logging::Logging::setup();
  dunedaq::iomanager::DatafilterConfig config;

  bool help_requested = false;
  namespace po = boost::program_options;
  po::options_description desc("Program to test IOManager load with many connections");
  desc.add_options()("use_connectivity_service,c",
                     po::bool_switch(&config.use_connectivity_service),
                     "enable the ConnectivityService in IOManager")(
      "next_tr,x",
      po::value<bool>(&config.next_tr)->default_value(config.next_tr),
      "get next TR")(
    "num_apps,N",
    po::value<size_t>(&config.num_apps)->default_value(config.num_apps),
    "Number of applications to start")("num_groups,g",
                                       po::value<size_t>(&config.num_groups)->default_value(config.num_groups),
                                       "Number of connection groups")(
    "num_connections,n",
    po::value<size_t>(&config.num_connections_per_group)->default_value(config.num_connections_per_group),
    "Number of connections to register and use in each group")(
    "port,p", po::value<int>(&config.port)->default_value(config.port), "port to connect to on configuration server")(
    "server,s",
    po::value<std::string>(&config.server)->default_value(config.server),
    "Configuration server to connect to")("num_messages,m",
                                          po::value<size_t>(&config.num_messages)->default_value(config.num_messages),
                                          "Number of messages to send on each connection")(
    "message_size_kb,z",
    po::value<size_t>(&config.message_size_kb)->default_value(config.message_size_kb),
    "Size of each message, in KB")("num_runs,r",
                                   po::value<size_t>(&config.num_runs)->default_value(config.num_runs),
                                   "Number of times to clear the sender and send all messages")(
    "publish_interval,i",
    po::value<int>(&config.publish_interval)->default_value(config.publish_interval),
    "Interval, in ms, for ConfigClient to re-publish connection info")(
    "send_interval,I",
    po::value<size_t>(&config.send_interval_ms)->default_value(config.send_interval_ms),
    "Interval, in ms, for Publishers to send messages")(
    "output_file_base,o",
    po::value<std::string>(&config.info_file_base)->default_value(config.info_file_base),
    "Base name for output info file (will have _sender.csv or _receiver.csv appended)")(
    "session",
    po::value<std::string>(&config.session_name)->default_value(config.session_name),
    "Name of this DAQ session")("help,h", po::bool_switch(&help_requested), "Print this help message");

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
//start fork process : we don't need it for now
//  std::vector<pid_t> forked_pids;
//  for (size_t ii = 0; ii < config.num_apps; ++ii) {
//    auto pid = fork();
//    if (pid < 0) {
//       TLOG() <<"fork error";
//       exit(EXIT_FAILURE);
//    } else if (pid == 0) { // child
//
//      forked_pids.clear();
//      config.my_id = ii;
//
//      TLOG() << "Datafilter : child process " << config.my_id <<"ii="<<ii;
//      break;
//    } else {
//        TLOG() << "Datafilter : parent process " << getpid();
//        forked_pids.push_back(pid);
//    }
//  }


//    std::this_thread::sleep_until(startup_time + 2s);

    TLOG() << "Datafilter" << config.my_id << ": "<< "Configuring IOManager";
    config.configure_iomanager();

    auto subscriber = std::make_unique<dunedaq::iomanager::SubscriberTest>(config);

    for (size_t run = 0; run < config.num_runs; ++run) {
      TLOG() << "Subscriber "  << config.my_id << ": "<< "Starting test run " << run;
      if (config.num_apps>1)
          subscriber->init(run);
      subscriber->receive(run);
      TLOG() << "Subscriber "  << config.my_id << ": "<< "Test run " << run << " complete.";
    }

    TLOG() << "Subscriber "  << config.my_id << ": " << "Cleaning up";
    subscriber.reset(nullptr);

    dunedaq::iomanager::IOManager::get()->reset();
    TLOG() << "Subscriber "  << config.my_id << ": "
           << "DONE";

//  if (forked_pids.size() > 0) {
//    TLOG() << "Waiting for forked PIDs";
//
//    for (auto& pid : forked_pids) {
//      siginfo_t status;
//      auto sts = waitid(P_PID, pid, &status, WEXITED);
//
//      TLOG_DEBUG(6) << "Forked process " << pid << " exited with status " << status.si_status << " (wait status " << sts
//                    << ")";
//    }
//  }
};
