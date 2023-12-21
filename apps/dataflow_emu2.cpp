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

//#include <pybind11/numpy.h>

//namespace py = pybind11;

using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::detdataformats;

namespace dunedaq {
namespace iomanager {


struct DatafilterConfig
{

  bool use_connectivity_service = false;  //unsed for now
  int port = 5000;
  std::string server = "localhost";

  std::string info_file_base = "dataflow_emu";
  std::string session_name = "iomanager : dataflow emulator";
  size_t num_apps = 1;
  size_t num_connections_per_group = 1;
  size_t num_groups = 1;
  size_t num_messages = 1;
  size_t message_size_kb = 1024;
  size_t num_runs = 2;
  size_t my_id = 0;
  size_t send_interval_ms = 100;
  int publish_interval = 10000;

  size_t seq_number;
  size_t trigger_number;
  size_t trigger_timestamp;
  size_t run_number;
  size_t element_id;
  size_t detector_id;
  size_t error_bits;
  size_t fragment_type;

  std::string input_h5_filename = "/lcg/storage19/test-area/dune/trigger_records/swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";
  std::string output_h5_filename = "/opt/tmp/chen/h5_test.hdf5";
  size_t write_fragment_type = 1; // 0 -> TPC; 1 -> Trigger
  //std::string hw_map_file_name="testwriter_hardwaremap.txt";
                                                
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


    IOManager::get()->configure(
      queues, connections, use_connectivity_service, std::chrono::milliseconds(publish_interval));
  }
};

struct PublisherTest
{
    struct PublisherInfo
    {
      size_t conn_id;
      size_t group_id;
      size_t messages_sent{ 0 };
      size_t trigger_number;
      size_t trigger_timestamp;
      size_t run_number;
      size_t element_id;
      size_t detector_id;
      size_t error_bits;
      size_t fragment_type;
      std::string path_header;
  
      std::shared_ptr<SenderConcept<dunedaq::datafilter::Data>> sender;
      std::unique_ptr<std::thread> send_thread;
      std::chrono::milliseconds get_sender_time;
  
      PublisherInfo(size_t group, size_t conn)
        : conn_id(conn)
        , group_id(group)
      {
      }
    };
  
    std::vector<std::shared_ptr<PublisherInfo>> publishers;
    DatafilterConfig config;

    uint16_t data3[200000000];
    uint32_t nchannels=64;
    uint32_t nsamples=64;

    std::string path_header1;

    std::string input_h5_filename = "/lcg/storage19/test-area/dune/trigger_records/swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5";
    //HDF5RawDataFile h5_file(config.input_h5_filename);
    //HDF5RawDataFile h5_file1(input_h5_filename);
  
    explicit PublisherTest(DatafilterConfig c)
      : config(c)
    {
    }
    
    void open()
    {

          HDF5RawDataFile h5_file(config.input_h5_filename);
          
    }
  void send(size_t dataflow_run_number, pid_t subscriber_pid)
  {
    std::ostringstream ss;

    std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
    std::mutex tracking_mutex;

//    for (size_t group = 0; group < config.num_groups; ++group) {
//      for (size_t conn = 0; conn < config.num_connections_per_group; ++conn) {
        //auto info = std::make_shared<PublisherInfo>(group, conn);
        auto info = std::make_shared<PublisherInfo>(0, 0);
        publishers.push_back(info);
//      }
//    }


    TLOG_DEBUG(7) << "Getting publisher objects for each connection";
    std::for_each(std::execution::par_unseq,
                  std::begin(publishers),
                  std::end(publishers),
                  [=](std::shared_ptr<PublisherInfo> info) {
                    auto before_sender = std::chrono::steady_clock::now();
                    info->sender = dunedaq::get_iom_sender<dunedaq::datafilter::Data>(
                      config.get_connection_name(config.my_id, info->group_id, info->conn_id));
                    auto after_sender = std::chrono::steady_clock::now();
                    info->get_sender_time =
                      std::chrono::duration_cast<std::chrono::milliseconds>(after_sender - before_sender);
                  });

    auto size=1024;
    std::vector<int> send_values;
//    std::generate(send_values.begin(),send_values.end(), []() {
//            return rand();
//            });

    for (auto j = 0; j < size*1024; j++)
    { 

        send_values.push_back(rand());
    }


    TLOG_DEBUG(7) << "Starting publish threads";
    std::for_each(
      std::execution::par_unseq,
      std::begin(publishers),
      std::end(publishers),
      [=, &completed_receiver_tracking, &tracking_mutex](std::shared_ptr<PublisherInfo> info) {
        info->send_thread.reset(new std::thread([=, &completed_receiver_tracking, &tracking_mutex]() {
          bool complete_received = false;

    HDF5RawDataFile h5_file(config.input_h5_filename);
    auto run_number = h5_file.get_attribute<unsigned int>("run_number");
    auto file_index = h5_file.get_attribute<unsigned int>("file_index");
    auto creation_timestamp = h5_file.get_attribute<std::string>("creation_timestamp");
    auto app_name = h5_file.get_attribute<std::string>("application_name");
 
    auto records = h5_file.get_all_record_ids();

   while (!complete_received) {
    for (auto const& rid : records) {
  		auto record_header_dataset = h5_file.get_record_header_dataset_path(rid);
        auto tr = h5_file.get_trigger_record(rid);
     	auto trh_ptr = h5_file.get_trh_ptr(rid);
        auto trig_num = trh_ptr->get_trigger_number();
        auto num_requested_components = trh_ptr->get_num_requested_components();
 
        auto seq_num = trh_ptr->get_sequence_number();
        auto max_seq_num = trh_ptr->get_max_sequence_number();

        path_header1 = record_header_dataset;
        config.seq_number=seq_num;
        config.trigger_number=trig_num;

        auto frag_paths =h5_file.get_fragment_dataset_paths(rid);
        auto all_frag_paths = h5_file.get_all_fragment_dataset_paths();
        for (auto const& path : frag_paths ) {
            auto frag_ptr = h5_file.get_frag_ptr(path);
            auto fragment_size=frag_ptr->get_size();
            auto fragment_type = frag_ptr->get_fragment_type();
            
            auto elem_id = frag_ptr->get_element_id();

            //config.element_id=elem_id;
            auto data = frag_ptr->get_data();

            //std::vector<uint64_t*>& data1 = *reinterpret_cast<std::vector<uint64_t*> *>(data);

            int nframes = (fragment_size-sizeof(daqdataformats::FragmentHeader))/sizeof(fddetdataformats::WIBEthFrame);
                  //std::cout<<"fragment_size"<<fragment_size<<" "<<nframes<<"\n";
                  for (auto i=0;i<nframes;++i){
                      auto fr = reinterpret_cast<fddetdataformats::WIBEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::WIBEthFrame));
                      for (auto j=0;j<nsamples;++j){
                          for (auto k=0;k<nchannels;++k){
                               data3[(nsamples*nchannels)*i + nchannels*j + k]=fr->get_adc(k,j);
                               //std::cout<<"=======================>"<<i<<" "<<j<<" "<<data3[nsamples*nchannels*i + nchannels*j+k]<<'\n';
                          }
                      }
                  }
//        }
//    }


//          while (!complete_received) {
            TLOG() << "Sending message " << info->messages_sent << " with size " << config.message_size_kb * 1024
                          << " bytes to connection "
                          << config.get_connection_name(config.my_id, info->group_id, info->conn_id);

            info->trigger_number=config.trigger_number;
            info->run_number=run_number;
            info->path_header=path;

            dunedaq::datafilter::Data d(info->messages_sent, info->trigger_number, info->trigger_timestamp,
                   info->run_number, info->element_id, info->detector_id, info->error_bits, info->fragment_type,
                   info->path_header, config.my_id, info->group_id, info->conn_id, config.message_size_kb * 1024);
            auto v1=999;
            for (auto j = 0; j < size*1024; j++)
            { 
                //v1=rand();
                //d.contents.push_back(v1);
                d.contents[j] = data3[j];
            }

            TLOG() <<"====> Trigger_number send: "<<config.trigger_number;
            TLOG() <<"====> run number send: "<<run_number<<"\n";
            TLOG() <<"====> record_header_dataset send: " <<path<<"\n";
            TLOG() <<"Print first 20 entries of a frame";
            for (auto ii = 0; ii < 20; ii++)
            { 
                TLOG() <<"Sender contents =>"<<d.contents[ii];
            }


            info->sender->try_send(std::move(d), std::chrono::milliseconds(config.send_interval_ms));
            ++info->messages_sent;
            {
              std::lock_guard<std::mutex> lk(tracking_mutex);
              if ((completed_receiver_tracking.count(info->group_id) &&
                   completed_receiver_tracking[info->group_id].count(info->conn_id)) ||
                  completed_receiver_tracking.count(-1)) {
                complete_received = true;
              }
            }
//            if (!check_subscriber()) {
//              TLOG_DEBUG(7) << "Subscriber app has gone away.";
          //    complete_received = true;
//            }
          }
          
              
          
            }
              //force the while loop to end when no trigger path left.
              complete_received = true;
    }     
        }));
      });

    TLOG_DEBUG(7) << "Joining send threads";
    for (auto& sender : publishers) {
      sender->send_thread->join();
      sender->send_thread.reset(nullptr);
    }

  }
};

}
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Data, "data_t");
}


int
main(int argc, char* argv[])
{
    dunedaq::logging::Logging::setup();
    dunedaq::iomanager::DatafilterConfig config;
  
    bool help_requested = false;
    namespace po = boost::program_options;
    po::options_description desc("Dataflow emulator");
    desc.add_options()("use_connectivity_service,c",
                       po::bool_switch(&config.use_connectivity_service),
                       "enable the ConnectivityService in IOManager")(
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
      "output_h5_filename,o",
      po::value<std::string>(&config.output_h5_filename)->default_value(config.output_h5_filename),
      "Base name for output info file (will have _sender.csv or _receiver.csv appended)")(
      "input_h5_filename,f",
      po::value<std::string>(&config.input_h5_filename)->default_value(config.input_h5_filename),
      "Input hdf5 file")(
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
  
   
//    auto startup_time = std::chrono::steady_clock::now();
// start fork process
//    std::vector<pid_t> forked_pids;
//    for (size_t ii = 0; ii < config.num_apps; ++ii) {
//      auto pid = fork();
//      std::cout<<"forked pid "<<pid<<" ii "<<ii<<"\n";
//      if (pid<0) {
//          TLOG()<<"fork error";
//          exit(EXIT_FAILURE);
//      } else if (pid == 0) { // child
//          forked_pids.clear();
//          config.my_id = ii;
//    
//          TLOG() << "Dataflow emulator: child process " << config.my_id;
//          break;
//      } else { //parent
//          TLOG() << "Datalfow emulator: parent process " << getpid();
//          forked_pids.push_back(pid);
//      }
//    }
  
//      std::this_thread::sleep_until(startup_time + 2s);
  
      TLOG() << "Dataflow emulator " << config.my_id << ": " << "Configuring IOManager";
      config.configure_iomanager();
  
      auto publisher = std::make_unique<dunedaq::iomanager::PublisherTest>(config);
  
      for (size_t run = 0; run < config.num_runs; ++run) {
        TLOG() << "Dataflow emulator" << config.my_id << ": "<< "run " << run;
        //publisher->send(run, forked_pids[0]);
        publisher->send(run, 0);
        TLOG() << "Dataflow emulator " << config.my_id << ": "<< "run " << run << " complete.";
      }
  
      TLOG() << "Dataflow emulator" << config.my_id << ": "<< "Cleaning up";
      publisher.reset(nullptr);
  
      dunedaq::iomanager::IOManager::get()->reset();
      TLOG() << "Dataflow emulator " << config.my_id << ": "<< "DONE";
  
//    if (forked_pids.size() > 0) {
//      TLOG() << "Waiting for forked PIDs";
//  
//      for (auto& pid : forked_pids) {
//        siginfo_t status;
//        auto sts = waitid(P_PID, pid, &status, WEXITED);
//  
//        TLOG_DEBUG(6) << "Forked process " << pid << " exited with status " << status.si_status << " (wait status " << sts
//                      << ")";
//      }
//    }
};
