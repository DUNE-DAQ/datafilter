/*
Datafilter : writer tests.
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

void print_usage()
{
    TLOG() << "Usage: hdf5libs_testwriter_test <input_h5_file_name> <output_h5_file_name> <write_fragment_type>";
}

int main(int argc, char** argv)
{
    if (argc != 4) {
      print_usage();
      return 1;
    }

    const std::string input_h5_filename = std::string(argv[1]);
    const std::string output_h5_filename = std::string(argv[2]);
    auto write_fragment_type = std::stoi(argv[3]); // 0 -> TPC; 1 -> Trigger
  
    auto cnt=0;  // "/" counter 
    bool is_replace = 0;
  
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
    //auto all_trigger_record_numbers = h5_file.get_all_trigger_record_numbers();
  
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
  
    //  //get datasets
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
  
    // create output file for writing
    HDF5RawDataFile h5_raw_data_file = HDF5RawDataFile(output_h5_filename,
                                                       run_number,                 // run_number
                                                       file_index,                 // file_index,
                                                       app_name,                   // app_name
                                                       flp,                    // file_layout_confs
  						     ".writing",                 // optional: suffix to use for files being written
                                                       HighFive::File::Overwrite); // optional: overwrite existing file
                                                                                   //
    for (auto const& rid : records) {
  		auto record_header_dataset = h5_file.get_record_header_dataset_path(rid);
        auto tr = h5_file.get_trigger_record(rid);
		auto trh_ptr = h5_file.get_trh_ptr(rid);
     	ss << "\n trh_ptr \t" << trh_ptr->get_header();
        ss << "\n record_header_dataset \t"<<record_header_dataset;
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
  
    	//TLOG() << ss.str(); ss.str("");
  
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
   
        auto frag_paths =h5_file.get_fragment_dataset_paths(rid);
        auto all_frag_paths = h5_file.get_all_fragment_dataset_paths();
        for (auto const& path : frag_paths ) {
            auto frag_ptr = h5_file.get_frag_ptr(path);
            auto fragment_size=frag_ptr->get_size();
            auto fragment_type = frag_ptr->get_fragment_type();
  
            auto elem_id = frag_ptr->get_element_id();
  
            std::vector<std::unique_ptr<Fragment>> frag_ptr1; 
  
            if ( write_fragment_type==0 )
               std::vector<char> dummy_data1(fragment_size-80 ,0);
            else
                fragment_size=80;
               std::vector<char> dummy_data1(fragment_size ,0);
  
            for (auto& i: dummy_data1 ) {
                i=1;
            }

            //TLOG()<<"<====> path "<<path;
  
            std::istringstream isSS(path);
            std::string token;
  
            cnt=0;
            while (std::getline(isSS,token,'/')) {
                if (!token.empty()) {
                    cnt++;
                    if (write_fragment_type==0 && cnt==4) {
                        if (token=="Link02") {
                            is_replace=1;
                        } else {
                            //TLOG()<<"<<==>>token "<<token<<" path "<<path;
                            is_replace=0;
                        } 
                    }
                    if (write_fragment_type==1 && cnt==4) {                    
                        if (token == "Element00001") {
                            TLOG()<<"<<==>>"<<token<<" will be removed from path "<<path;
                            is_replace=1;
                        } else {
                            is_replace=0;
                        }
                    }
                  }
            }
  
               if (is_replace==1) {
                  //std::vector<std::unique_ptr<Fragment>> frag_ptr1; 
                  //std::vector<std::pair<void*, size_t>> list_of_pieces(fragment_size,std::make_pair(void*,0));
                  //std::vector<std::pair<void*, size_t>> list_of_pieces;
                  //for (auto& list_of_piece : list_of_pieces)
                  //    TLOG()<<"\n list_of_piece \t"<<list_of_piece.first;
 
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
                  //frag->set_type(frag_ptr->get_fragment_type());
                  //frag->set_detector_id(frag_ptr->get_detector_id());
                  //frag->set_run_number(run_number);
                  //frag->set_trigger_number(trig_num);
                  //frag->set_window_begin(ts-10);
                  //frag->set_window_end(ts);
                  //frag->set_element_id(elem_id);
                  //frag->set_type(daqdataformats::FragmentType::kTriggerPrimitives);
                  //frag->set_header_fields(frag_ptr->get_header());
                  //
                  //frag_ptr1.push_back(std::move(frag));
                  //  //frag_ptr1->set_header_fields(frag_ptr->get_header());
                  // 
                  //  //int num_frames = fragment_size/sizeof(detdataformats::wib::WIBFrame);
  
                  auto frag_ptr2 = std::make_unique<Fragment>(dummy_data1.data(), dummy_data1.size());
                  //frag_ptr2->set_header_fields(fh);
                  frag_ptr2->set_header_fields(frag_ptr->get_header());
 
                  //auto record_number = h5_file.get_file_layout().get_record_number_string(trig_num,seq_num);
                  //std::cout<<"\n =====record_number_string \t"<<record_number; 
                  //frag_ptr2->set_header_fields(frag->get_header());
                  if (write_fragment_type==0) {
                      TLOG()<<"Link02 found in path and will be replaced by a vector with 1 in " << path;
                      tr1.add_fragment(std::move(frag_ptr2));
                  }
        } else {
                 
                  tr1.add_fragment(std::move(frag_ptr));
        }
  
        //dummy_data1.clear();
      }   // end loop over regions
  
  
      // write trigger record to file
      //if (trig_num%10!=0) {
      h5_raw_data_file.write(tr1);
    } // end loop over triggers
  
    TLOG() << "Finished writing to file " << h5_raw_data_file.get_file_name();
    TLOG() << "Recorded size: " << h5_raw_data_file.get_recorded_size();
  
    return 0;
}
