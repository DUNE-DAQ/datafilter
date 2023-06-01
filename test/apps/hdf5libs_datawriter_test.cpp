/*
Datafiler : writer tests.
*/

#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/hdf5filelayout/Nljs.hpp"
#include "detdataformats/wib/WIBFrame.hpp"
#include "detdataformats/wib2/WIB2Frame.hpp"

#include "logging/Logging.hpp"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <sstream>

using namespace dunedaq::hdf5libs;
using namespace dunedaq::daqdataformats;
using namespace dunedaq::detdataformats;

void
print_usage()
{
  TLOG() << "Usage: hdf5libs_testwriter_test <input_h5_file_name> <output_h5_file_name>";
}

int
main(int argc, char** argv)
{

  if (argc != 3) {
    print_usage();
    return 1;
  }

  const std::string input_h5_filename = std::string(argv[1]);
  const std::string output_h5_filename = std::string(argv[2]);

  //open h5 file
  HDF5RawDataFile h5_file(input_h5_filename);
  
  std::ostringstream ss;
  
  ss << "\nFile name: " << h5_file.get_file_name();
  ss << "\n\tRecorded size from class: " << h5_file.get_recorded_size();
  
  auto recorded_size = h5_file.get_attribute<size_t>("recorded_size");
  ss << "\n\tRecorded size from attribute: " << recorded_size;
  
  auto record_type = h5_file.get_record_type();
  ss << "\nRecord type = " << record_type;
  
  nlohmann::json flp_json;
  auto flp = h5_file.get_file_layout().get_file_layout_params();

  hdf5filelayout::to_json(flp_json, flp);
  ss << "\nFile Layout Parameters:\n" << flp_json;
  
  // get some attributs from h5_file
  auto run_number = h5_file.get_attribute<unsigned int>("run_number");
  auto file_index = h5_file.get_attribute<unsigned int>("file_index");
  auto creation_timestamp = h5_file.get_attribute<std::string>("creation_timestamp");
  auto app_name = h5_file.get_attribute<std::string>("application_name");
  auto all_trigger_record_numbers = h5_file.get_all_trigger_record_numbers();

  ss << "\n Run number = " << run_number;
  ss << "\n File index: " << file_index;
  ss << "\n Application name: " << app_name;
  //ss << "\n all_trigger_record_numbers"<<all_trigger_record_numbers;

  TLOG() << ss.str();
  ss.str("");


  const int trigger_count = recorded_size;
  auto records = h5_file.get_all_record_ids();
  ss << "\nNumber of records: " << records.size();
  if (records.empty()) {
  	ss << "\n\nNO TRIGGER RECORDS FOUND";
  	TLOG() << ss.str();
  	return 0;
  }
  auto first_rec = *(records.begin());
  auto last_rec = *(std::next(records.begin(), records.size() - 1));

  ss << "\n\tFirst record: " << first_rec.first << "," << first_rec.second;
  ss << "\n\tLast record: " << last_rec.first << "," << last_rec.second;
  
  TLOG() << ss.str();
  ss.str("");

  auto all_rh_paths = h5_file.get_record_header_dataset_paths();
  ss << "\nAll record header datasets found:";
  for (auto const& path : all_rh_paths)
  	ss << "\n\t" << path;
  TLOG() << ss.str();
  ss.str("");

  if(h5_file.is_trigger_record_type()) {
  	auto trh_ptr = h5_file.get_trh_ptr(first_rec);
  	ss << "\nTrigger Record Headers:";
  	ss << "\nFirst: " << trh_ptr->get_header();
  	ss << "\nLast: " << h5_file.get_trh_ptr(all_rh_paths.back())->get_header();
  } else if(h5_file.is_timeslice_type()) {
  	auto tsh_ptr = h5_file.get_tsh_ptr(first_rec);
  	ss << "\nTimeSlice Headers:";
  	ss << "\nFirst: " << *tsh_ptr;
  	ss << "\nLast: " << *(h5_file.get_tsh_ptr(all_rh_paths.back()));
  }
  TLOG() << ss.str();
  ss.str("");

//    //get datasets
//	for(auto const& rid : h5_file.get_all_record_ids()){
//		ss << "Processing record (" << rid.first << "," << rid.second << "):";
//
//		auto record_header_dataset = h5_file.get_record_header_dataset_path(rid);
//		if(h5_file.is_trigger_record_type()){
//			auto trh_ptr = h5_file.get_trh_ptr(rid);
//			ss << "\n\t" << trh_ptr->get_header();
//		} else if(h5_file.is_timeslice_type()){
//			auto tsh_ptr = h5_file.get_tsh_ptr(rid);
//			ss << "\n\t" << *tsh_ptr;
//		}
//
//		for(auto const& gid : h5_file.get_geo_ids(rid)){
//			//ss << "\n\t" << gid << ": ";
//			auto frag_ptr = h5_file.get_frag_ptr(rid,gid);
//			ss << "\n\t" << frag_ptr->get_header();
//		}
//
//		//could also do loop like this...
//		//for(auto const& frag_dataset : h5_raw_data_file.get_fragment_dataset_paths(rid))
//		//  auto frag_ptr = h5_raw_data_file.get_frag_ptr(frag_dataset)
//
//		TLOG() << ss.str(); ss.str("");
//	}
//

  // open our file for writing
  HDF5RawDataFile h5_raw_data_file = HDF5RawDataFile(output_h5_filename,
                                                     run_number,                 // run_number
                                                     file_index,                 // file_index,
                                                     app_name,                   // app_name
                                                     flp,                    // file_layout_confs
						     ".writing",                 // optional: suffix to use for files being written
                                                     HighFive::File::Overwrite); // optional: overwrite existing file
  // open our file for writing
  std::string output_h5_filename2 = "/opt/tmp/chen/h5_test.hdf5";

  HDF5RawDataFile h5_raw_data_file2 = HDF5RawDataFile(output_h5_filename2,
                                                     run_number,                 // run_number
                                                     file_index,                 // file_index,
                                                     app_name,                   // app_name
                                                     flp,                    // file_layout_confs
						     ".writing",                 // optional: suffix to use for files being written
                                                     HighFive::File::Overwrite); // optional: overwrite existing file


  // loop over desired number of triggers
  bool is_replace = 0;
  int fragment_size=80;
  std::vector<char> dummy_data(fragment_size ,0);

  for (auto& i: dummy_data ) {
      i=1;
  }
//         for (auto i : dummy_data) {
//            std::cout<<"\n dummy_data\t" <<i;
            //std::cout<<"\n buffer \t" <<((char *) buffer)[i];
//        }

//std::vector<char*> v(10, "hi!");    //  You cannot put standard library containers e.g. char[] into std::vector!
//for (auto& i : v)
//{
//    //std::cout << i << std::endl;
//    i = "New";
//}
//for (auto i : v)
//{
//    std::cout << i << std::endl;
//}
  //auto trh_ptr=0;
  //for (int trig_num = 1; trig_num <= trigger_count; ++trig_num) {
    for (auto const& rid : records) {
        TLOG() << "rid" <<rid.first;
    // get a timestamp for this trigger
    //uint64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>( // NOLINT(build/unsigned)
    //                system_clock::now().time_since_epoch())
    //                .count();
		auto record_header_dataset = h5_file.get_record_header_dataset_path(rid);
        auto tr = h5_file.get_trigger_record(rid);
//		if(h5_file.is_trigger_record_type()){
			auto trh_ptr = h5_file.get_trh_ptr(rid);
//            auto trh = trh_ptr->get_header();
			ss << "\n trh_ptr \t" << trh_ptr->get_header();
            ss << "\n record_header_dataset \t"<<record_header_dataset;
//		} else if(h5_file.is_timeslice_type()){
//			auto tsh_ptr = h5_file.get_tsh_ptr(rid);
//			ss << "\n\t" << *tsh_ptr;
//		}
          // get a timestamp for this trigger
          uint64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>( // NOLINT(build/unsigned)
                          system_clock::now().time_since_epoch())
                          .count();

         auto trig_num = trh_ptr->get_trigger_number();
         auto num_requested_components = trh_ptr->get_num_requested_components();
         ss <<"\n num_requested_components \t"<<num_requested_components;
         auto seq_num = trh_ptr->get_sequence_number();
         auto max_seq_num = trh_ptr->get_max_sequence_number();
         ss <<"\n seq_num \t"<<seq_num;

		TLOG() << ss.str(); ss.str("");

        // create TriggerRecordHeader
        TriggerRecordHeaderData trh_data;
        trh_data.trigger_number = trig_num;
        trh_data.trigger_timestamp = ts;
        trh_data.num_requested_components = num_requested_components;
        trh_data.run_number = run_number;
        trh_data.sequence_number = seq_num;
        trh_data.max_sequence_number = max_seq_num;
     
        TriggerRecordHeader trh1(&trh_data);
        // create out TriggerRecord
        TriggerRecord tr1(trh1);
 
    //TLOG() << "\tWriting trigger " << trig_num << " with time_stamp " << ts;
    //TLOG() << "\tWriting trigger " << trig_num;
    // loop over regions and elements
//    for (int reg_num = 0; reg_num < region_count; ++reg_num) {
//      for (int ele_num = 0; ele_num < element_count; ++ele_num) {

     //auto frag_path = h5_file.get_fragment_dataset_paths(rid);
     //TLOG() <<"\n frag_path \t" <<frag_path;
//		for(auto const& frag_paths : h5_file.get_fragment_dataset_paths(rid))
//		  auto frag_path = h5_file.get_frag_ptr(frag_paths);
//          //TLOG() <<"\n ====>frag_path \t"<<frag_path;
          
//    auto all_frag_paths = h5_file.get_all_fragment_dataset_paths();
//    ss << "\nAll fragment datasets found:";
//    for (auto const& path : all_frag_paths)
//    	ss << "\n path \t" << path;
//    TLOG() << ss.str();
//    ss.str("");
//
//     auto  frag_sid_list = h5_file.get_fragment_dataset_paths(rid);
//    for (auto const& source_id : frag_sid_list) {
//      auto frag_ptr = h5_file.get_frag_ptr(rid, source_id);
//      ss << "\n\t" << fragment_type_to_string(frag_ptr->get_fragment_type()) << " fragment with SourceID "
//         << frag_ptr->get_element_id().to_string() << " from subdetector "
//         << DetID::subdetector_to_string(static_cast<DetID::Subdetector>(frag_ptr->get_detector_id()))
//         << " has size = " << frag_ptr->get_size();
//      if (frag_ptr->get_element_id().subsystem == SourceID::Subsystem::kDetectorReadout) {
//        ss << "\n\t\t"
//           << "It may contain data from the following detector components:";
//        std::vector<uint64_t> geo_id_list = h5_file.get_geo_ids_for_source_id(rid, source_id);
//        for (auto const& geo_id : geo_id_list) {
//          HardwareMapService::GeoInfo geo_info = HardwareMapService::parse_geo_id(geo_id);
//          ss << "\n\t\t\t"
//             << "subdetector " << DetID::subdetector_to_string(static_cast<DetID::Subdetector>(geo_info.det_id))
//             << ", crate " << geo_info.det_crate << ", slot " << geo_info.det_slot << ", link " << geo_info.det_link;
//        }
//      }
//    }
//    TLOG() << ss.str();
//    ss.str("");
      std::unique_ptr<Fragment> frag(new Fragment((void*) &dummy_data,fragment_size));
      auto frag_paths =h5_file.get_fragment_dataset_paths(rid);
      //auto frag_path = frag_paths[trig_num];
      auto all_frag_paths = h5_file.get_all_fragment_dataset_paths();
      for (auto const& path : all_frag_paths ) {
//      for (auto const& gid : h5_file.get_geo_ids(rid)) {
       // auto gid=h5_file.get_geo_ids(rid);
        //TLOG() <<"\n gid \t" <<gid;

          auto frag_ptr = h5_file.get_frag_ptr(path);
          auto fragment_size=frag_ptr->get_size();
          auto fragment_type = frag_ptr->get_fragment_type();

          auto elem_id = frag_ptr->get_element_id();

          std::istringstream isSS(path);
          std::vector<std::string> tokens;
          std::string token;

          while (std::getline(isSS,token,'/')) {
              if (!token.empty())
                 tokens.push_back(token);
          }
        
         
          for ( auto i : tokens ) {
             if ( i=="Link02") {
                std::vector<std::unique_ptr<Fragment>> frag_ptr1; 
                //std::vector<std::pair<void*, size_t>> list_of_pieces(fragment_size,std::make_pair(void*,0));
                //std::vector<std::pair<void*, size_t>> list_of_pieces;
                //for (auto& list_of_piece : list_of_pieces)
                //    TLOG()<<"\n list_of_piece \t"<<list_of_piece.first;

                //void * buffer=&dummy_data;
                
                //for (int i;i<fragment_size;i++) 
                //    std::cout<<" \t buffer"<<*buffer;

                // create our fragment
                FragmentHeader fh;
                fh.trigger_number = trig_num;
                fh.trigger_timestamp = ts;
                fh.window_begin = ts - 10;
                fh.window_end = ts;
                fh.run_number = run_number;
                fh.fragment_type = int(fragment_type);
             	fh.sequence_number = seq_num;
                //fh.element_id = GeoID(gtype_to_use, reg_num, ele_num);
                fh.element_id = elem_id;

                //std::unique_ptr<Fragment> frag(new Fragment(list_of_pieces));
               
// this is another way to set the fragment header
//                frag->set_type(frag_ptr->get_fragment_type());
                //frag->set_detector_id(frag_ptr->get_detector_id());
//                frag->set_run_number(run_number);
//                frag->set_trigger_number(trig_num);
//                frag->set_window_begin(ts-10);
//                frag->set_window_end(ts);
//                frag->set_element_id(elem_id);
                //frag->set_type(daqdataformats::FragmentType::kTriggerPrimitives);
                //frag->set_header_fields(frag_ptr->get_header());
//
                //frag_ptr1.push_back(std::move(frag));
//                //frag_ptr1->set_header_fields(frag_ptr->get_header());
//               
//                //int num_frames = fragment_size/sizeof(detdataformats::wib::WIBFrame);
	            auto frag_ptr2 = std::make_unique<Fragment>(dummy_data.data(), dummy_data.size());
                frag_ptr2->set_header_fields(fh);
                //frag_ptr2->set_header_fields(frag_ptr->get_header());

                //char * buffer new char[fragment_size];
                
//                void * buffer = frag_ptr2->get_data();
//                for (int i=0;i<10;i++) {
//                   std::cout<<"\n =====buffer \t"<<((char *) buffer)[i]; 
//                }

                //auto record_number = h5_file.get_file_layout().get_record_number_string(trig_num,seq_num);
                //std::cout<<"\n =====record_number_string \t"<<record_number; 
                //frag_ptr2->set_header_fields(frag->get_header());
                tr1.add_fragment(std::move(frag_ptr2));
                //tr.set_fragments(std::move(frag_ptr1));
                is_replace = 1;

                 h5_raw_data_file2.write(tr1);
                //if (elem_id == 4) {
                    TLOG()<<"Link02 found in path and will be replace by zero vector in " << path;
                //}
             } else {
                //do something here   
                // this is another way to set the fragment header
//                frag->set_type(frag_ptr->get_fragment_type());
//                //frag->set_detector_id(frag_ptr->get_detector_id());
//                frag->set_run_number(run_number);
//                frag->set_trigger_number(trig_num);
//                frag->set_window_begin(ts-10);
//                frag->set_window_end(ts);
//                frag->set_element_id(elem_id);
//                //frag->set_type(daqdataformats::FragmentType::kTriggerPrimitives);
//                //frag->set_header_fields(frag_ptr->get_header());
//                tr.add_fragment(std::move(frag));
             }

             if (i=="Trigger") {
                 //TLOG()<<"\n Trigger found to be replaced\t"<<path;
             }

             //dummy_data.clear();
          }
                                  
          //TLOG() <<"\n =============>gid"<<gid<<"frag_hdr "<<frag_hdr<<"<==>elem_id \t"<<elem_id;
       //   TLOG() <<"\n ==>> gid"<<gid<<"trig_num "<<trig_num<<"frag_path " <<frag_path<<" elem_id"<<elem_id;
//      } // end loop over elements
    }   // end loop over regions

    // write trigger record to file
    //if (trig_num%10!=0) {
//     if (is_replace) {
//    
//         //tr1.add_fragment(std::move(frag_ptr2));
////         std::cout<<"\n ===>write tr1 \t";
//         h5_raw_data_file2.write(tr1);
//      } else {
//          h5_raw_data_file.write(tr);
////       std::cout<<"\n ===>write tr \t";
////      }
//    }

  } // end loop over triggers

  TLOG() << "Finished writing to file " << h5_raw_data_file.get_file_name();
  TLOG() << "Recorded size: " << h5_raw_data_file.get_recorded_size();

  return 0;
}
