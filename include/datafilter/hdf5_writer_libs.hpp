#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/hdf5filelayout/Nljs.hpp"

#include "logging/Logging.hpp"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "nlohmann/json.hpp"

using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
//using namespace dunedaq::detdataformats;
//namespace nl nlohmann;

void
print_usage(nlohmann::json fl_conf_in1, std::string ifile_name, std::string hw_map_file_name, std::string ofile_name)
{

    nlohmann::json fl_conf;

    //fl_conf = fl_conf_in["file_layout"].get<hdf5filelayout::FileLayoutParams>();
    //fl_conf = fl_conf_in["file_layout"].get<std::string>();
//    const int file_index = fl_conf_in["file_index"].get<int>();
    nlohmann::json j_in, fl_conf1;
    std::ifstream ifile(ifile_name);
    ifile >> j_in;
    ifile.close();

    nlohmann::json j = "{ \"happy\": true, \"pi\": 3.141 }"_json;
    auto j3 = nlohmann::json::parse(j.dump());
    TLOG() <<"j"<<j<< "j3\n"<<j3;
    //convert json string to json
    nlohmann::json fl_conf_in = nlohmann::json::parse(fl_conf_in1.dump());

    TLOG() <<"j_in \n "<<j_in;

    TLOG() <<"print_usage arg \n"<<ifile_name;
    TLOG() <<"fl_conf_in \n"<<fl_conf_in;
    //TLOG() <<"print_usage arg \n"<<fl_conf;
    TLOG() <<"print_usage arg \n"<<hw_map_file_name;
    TLOG() <<"print_usage arg \n"<<ofile_name;
    //const int run_number = fl_conf_in["run_number"].get<int>();
    //TLOG() <<"print_usage \n"<<run_number;
//    TLOG() <<"print_usage \n"<<run_number<<"   file_index " <<file_index;
    TLOG() << "Usage: HDF5LIBS_TestWriter <configuration_file> <hardware_map_file> <output_file_name>";
}

void
data_writer2(std::string ofile_name, int run_number, int file_index,std::string app_name,
         nlohmann::json fl_conf_in1, std::vector<std::string> dummy_data)
{
    std::string ifile_name="testwriter_conf.json";
    std::string hw_map_file_name="testwriter_hardwaremap.txt";

    TLOG() <<"print_usage arg \n"<<fl_conf_in1;
    TLOG() <<"print_usage arg \n"<<hw_map_file_name;
    TLOG() <<"print_usage arg \n"<<ofile_name;

    //convert json string to json
    nlohmann::json fl_conf_in = nlohmann::json::parse(fl_conf_in1.dump());
    nlohmann::json fl_conf_in2 = nlohmann::json::parse(fl_conf_in.dump());

    TLOG() <<"------>fl_conf_in1 \n"<<fl_conf_in;
    TLOG() <<"------>fl_conf_in2 \n"<<fl_conf_in2;

    // read in configuration
    nlohmann::json fl_conf1;

    nlohmann::json j_in, fl_conf;
    std::ifstream ifile(ifile_name);
    ifile >> j_in;
    ifile.close();

    std::string input_h5_filename = "/lcg/storage19/test-area/dune/trigger_records/np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5";
    //open h5 file
     HDF5RawDataFile h5_file(input_h5_filename);
 
    nlohmann::json flp_json;
    auto flp = h5_file.get_file_layout().get_file_layout_params();


    //hdf5filelayout::to_json(flp_json, flp);
  

    //const std::string app_name;

    // get file_layout config
//    try {
//      //app_name= fl_conf_in['application_name'];
//      //fl_conf = j_in["filelayout_params"].get<hdf5filelayout::FileLayoutParams>();
//      fl_conf = j_in["path_param_list"].get<hdf5filelayout::FileLayoutParams>();
//      //fl_conf1 = fl_conf_in1["path_param_list"].get<hdf5filelayout::FileLayoutParams>();
//
////      run_number = fl_conf_in["run_number"];
////      file_index = fl_conf_in["file_index"].get<int>();
//
////      trigger_count = fl_conf_in["trigger_count"].get<int>();
////      fragment_size = fl_conf_in["data_size"].get<int>() + sizeof(FragmentHeader);
////      stype_to_use = SourceID::string_to_subsystem(fl_conf_in["subsystem_type"].get<std::string>());
////      dtype_to_use = DetID::string_to_subdetector(fl_conf_in["subdetector_type"].get<std::string>());
////      ftype_to_use = string_to_fragment_type(fl_conf_in["fragment_type"].get<std::string>());
////      element_count = fl_conf_in["element_count"].get<int>();
//
//      TLOG() << "Read 'filelayout_params' configuration:\n";
//      TLOG() << fl_conf;
//    } catch (...) {
//      TLOG() << "ERROR: Improper 'filelayout_params' configuration in ";
//      //return 1;
//    }
//
    //const int trigger_count = fl_conf_in["trigger_count"].get<int>();
    int trigger_count = 1;
//    const int fragment_size = fl_conf_in["data_size"].get<int>() + sizeof(FragmentHeader);
//    const SourceID::Subsystem stype_to_use = SourceID::string_to_subsystem(fl_conf_in["subsystem_type"].get<std::string>());
//    const DetID::Subdetector dtype_to_use = DetID::string_to_subdetector(fl_conf_in["subdetector_type"].get<std::string>());
//    const FragmentType ftype_to_use = string_to_fragment_type(fl_conf_in["fragment_type"].get<std::string>());
//    const int element_count = fl_conf_in["element_count"].get<int>();

    // read test writer app configs
//    const int run_number = j_in["run_number"].get<int>();
//    file_index = j_in["file_index"].get<int>();
//
//    const int trigger_count = j_in["trigger_count"].get<int>();
//    const int fragment_size = j_in["data_size"].get<int>() + sizeof(FragmentHeader);
//    const SourceID::Subsystem stype_to_use = SourceID::string_to_subsystem(j_in["subsystem_type"].get<std::string>());
//    const GeoID::SystemType gtype_to_use = GeoID::string_to_system_type(j_in["fragment_type"].get<std::string>());
      const GeoID::SystemType gtype_to_use = GeoID::string_to_system_type("WIP");
//    const FragmentType ftype_to_use = string_to_fragment_type(j_in["fragment_type"].get<std::string>());
      const FragmentType ftype_to_use =string_to_fragment_type("WIP");
//    const int element_count = j_in["element_count"].get<int>();
      const int element_count = 14;
//
//    TLOG() << "\nOutput file: " << ofile_name << "\nRun number: " << run_number << "\nFile index: " << file_index
//           << "\nNumber of trigger records: " << trigger_count << "\nNumber of fragments: " << element_count
//           << "\nSubsystem: " << SourceID::subsystem_to_string(stype_to_use)
//           << "\nFragment size (bytes, incl. header): " << fragment_size;

    // create the HardwareMapService
//    std::shared_ptr<dunedaq::detchannelmaps::HardwareMapService> hw_map_service(
//      new dunedaq::detchannelmaps::HardwareMapService(hw_map_file_name));

    // open our file for writing
    HDF5RawDataFile h5_raw_data_file = HDF5RawDataFile(ofile_name,
                                                       run_number, // run_number
                                                       file_index, // file_index,
                                                       app_name,   // app_name
                                                       flp,    // file_layout_confs
//                                                       fl_conf_in1);
//                                                       hw_map_service,
                                                       ".writing", // optional: suffix to use for files being written
//                                                       " ",
//                                                       HighFive::File::ReadWrite); // optional: overwrite existing file
                                                       HighFive::File::Overwrite); // optional: overwrite existing file
//    int fragment_size=80;
//    std::vector<char> adc_data(fragment_size,0);
//    for (auto& i: adc_data) {
//       i=1; 
//   }
    // loop over desired number of triggers
    for (int trig_num = 1; trig_num <= trigger_count; ++trig_num) {

      // get a timestamp for this trigger
      uint64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>( // NOLINT(build/unsigned)
                      system_clock::now().time_since_epoch())
                      .count();

      TLOG() << "\tWriting trigger " << trig_num << " with time_stamp " << ts;

      // create TriggerRecordHeader
      TriggerRecordHeaderData trh_data;
      trh_data.trigger_number = trig_num;
      trh_data.trigger_timestamp = ts;
      trh_data.num_requested_components = element_count;
      trh_data.run_number = run_number;
      trh_data.sequence_number = 0;
      trh_data.max_sequence_number = 1;
      //trh_data.element_id = SourceID(SourceID::Subsystem::kTRBuilder, 0);

      TriggerRecordHeader trh(&trh_data);

      // create out TriggerRecord
      TriggerRecord tr(trh);

      // loop over elements
      for (int ele_num = 0; ele_num < element_count; ++ele_num) {

        // create our fragment
        FragmentHeader fh;
        fh.trigger_number = trig_num;
        fh.trigger_timestamp = ts;
        fh.window_begin = ts - 10;
        fh.window_end = ts;
        fh.run_number = run_number;
        //fh.fragment_type = static_cast<fragment_type_t>(ftype_to_use);
        fh.fragment_type = 0;
        fh.sequence_number = 0;
        //fh.detector_id = static_cast<uint16_t>(dtype_to_use);
        //fh.element_id = SourceID(stype_to_use, ele_num);
        fh.element_id = GeoID(gtype_to_use, trig_num, ele_num);

        auto frag_ptr = std::make_unique<Fragment>(dummy_data.data(), dummy_data.size());
        //auto frag_ptr = std::make_unique<Fragment>(adc_data.data(), adc_data.size());
        frag_ptr->set_header_fields(fh);

        // add fragment to TriggerRecord
        tr.add_fragment(std::move(frag_ptr));

      } // end loop over elements

      // write trigger record to file
      h5_raw_data_file.write(tr);

    } // end loop over triggers

   // TLOG() << "Finished writing to file " << h5_raw_data_file.get_file_name();
   // TLOG() << "Recorded size: " << h5_raw_data_file.get_recorded_size();


}

void
//data_writer(nlohmann::json fl_conf_in, std::string ifile_name, std::string hw_map_file_name, std::string ofile_name)
data_writer(nlohmann::json fl_conf_in, std::string ifile_name)
{

  TLOG() << "fl_conf_in====>\n",fl_conf_in, ifile_name;

  // read in configuration
  nlohmann::json j_in, fl_conf;
  std::ifstream ifile(ifile_name);
  ifile >> j_in;
  ifile.close();

  std::string app_name; 
  

//  // get file_layout config
//  try {
//    app_name= fl_conf_in['application_name']; 
//    fl_conf = fl_conf_in["filelayout_params"].get<hdf5filelayout::FileLayoutParams>();
//    TLOG() << "Read 'file_layout' configuration:\n";
//    TLOG() << fl_conf;
//  } catch (...) {
//    TLOG() << "ERROR: Improper 'file_layout' configuration in " << ifile_name;
//    return 1;
//  }
//
//  // read test writer app configs
//  const int run_number = j_in["run_number"].get<int>();
//  const int file_index = j_in["file_index"].get<int>();
//
//  const int trigger_count = j_in["trigger_count"].get<int>();
//  const int fragment_size = j_in["data_size"].get<int>() + sizeof(FragmentHeader);
//  const SourceID::Subsystem stype_to_use = SourceID::string_to_subsystem(j_in["subsystem_type"].get<std::string>());
//  const DetID::Subdetector dtype_to_use = DetID::string_to_subdetector(j_in["subdetector_type"].get<std::string>());
//  const FragmentType ftype_to_use = string_to_fragment_type(j_in["fragment_type"].get<std::string>());
//  const int element_count = j_in["element_count"].get<int>();
//
//  TLOG() << "\nOutput file: " << ofile_name << "\nRun number: " << run_number << "\nFile index: " << file_index
//         << "\nNumber of trigger records: " << trigger_count << "\nNumber of fragments: " << element_count
//         << "\nSubsystem: " << SourceID::subsystem_to_string(stype_to_use)
//         << "\nFragment size (bytes, incl. header): " << fragment_size;
//
//  // create the HardwareMapService
//  std::shared_ptr<dunedaq::detchannelmaps::HardwareMapService> hw_map_service(
//    new dunedaq::detchannelmaps::HardwareMapService(hw_map_file_name));
//
//  // open our file for writing
//  HDF5RawDataFile h5_raw_data_file = HDF5RawDataFile(ofile_name,
//                                                     run_number, // run_number
//                                                     file_index, // file_index,
//                                                     app_name,   // app_name
//                                                     fl_conf,    // file_layout_confs
//                                                     hw_map_service,
//                                                     ".writing", // optional: suffix to use for files being written
//                                                     HighFive::File::Overwrite); // optional: overwrite existing file
//
//  std::vector<char> dummy_data(fragment_size);
//
//  // loop over desired number of triggers
//  for (int trig_num = 1; trig_num <= trigger_count; ++trig_num) {
//
//    // get a timestamp for this trigger
//    uint64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>( // NOLINT(build/unsigned)
//                    system_clock::now().time_since_epoch())
//                    .count();
//
//    TLOG() << "\tWriting trigger " << trig_num << " with time_stamp " << ts;
//
//    // create TriggerRecordHeader
//    TriggerRecordHeaderData trh_data;
//    trh_data.trigger_number = trig_num;
//    trh_data.trigger_timestamp = ts;
//    trh_data.num_requested_components = element_count;
//    trh_data.run_number = run_number;
//    trh_data.sequence_number = 0;
//    trh_data.max_sequence_number = 1;
//    trh_data.element_id = SourceID(SourceID::Subsystem::kTRBuilder, 0);
//
//    TriggerRecordHeader trh(&trh_data);
//
//    // create out TriggerRecord
//    TriggerRecord tr(trh);
//
//    // loop over elements
//    for (int ele_num = 0; ele_num < element_count; ++ele_num) {
//
//      // create our fragment
//      FragmentHeader fh;
//      fh.trigger_number = trig_num;
//      fh.trigger_timestamp = ts;
//      fh.window_begin = ts - 10;
//      fh.window_end = ts;
//      fh.run_number = run_number;
//      fh.fragment_type = static_cast<fragment_type_t>(ftype_to_use);
//      fh.sequence_number = 0;
//      fh.detector_id = static_cast<uint16_t>(dtype_to_use);
//      fh.element_id = SourceID(stype_to_use, ele_num);
//
//      auto frag_ptr = std::make_unique<Fragment>(dummy_data.data(), dummy_data.size());
//      frag_ptr->set_header_fields(fh);
//
//      // add fragment to TriggerRecord
//      tr.add_fragment(std::move(frag_ptr));
//
//    } // end loop over elements
//
//    // write trigger record to file
//    h5_raw_data_file.write(tr);
//
//  } // end loop over triggers
//
//  TLOG() << "Finished writing to file " << h5_raw_data_file.get_file_name();
//  TLOG() << "Recorded size: " << h5_raw_data_file.get_recorded_size();
//
  //return 0;
}
