cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
project(datafilter VERSION 0.2.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

include_directories(pybind11_json/include)
include_directories(pybind11/include)

find_package(logging REQUIRED)
find_package(ers REQUIRED)
find_package(appfwk REQUIRED)
find_package(HighFive REQUIRED)
find_package(daqdataformats REQUIRED)
find_package(detdataformats REQUIRED)
find_package(fddetdataformats REQUIRED)
find_package(detchannelmaps REQUIRED)
find_package(hdf5libs REQUIRED)
find_package(dfmodules REQUIRED)
find_package(dfmessages REQUIRED)
find_package(iomanager REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(cetlib REQUIRED)   # Uses the daq-buildtools/cmake/Findcetlib.cmake
find_package(opmonlib REQUIRED)
find_package(serialization REQUIRED)
find_package(okssystem REQUIRED)
find_package(conffwk REQUIRED)
find_package(Boost COMPONENTS iostreams unit_test_framework REQUIRED)

#find_package(pybind11_json ${pybind11_json_REQUIRED_VERSION} REQUIRED)
include_directories(src/exts/nlohmann_json/include)

# daq_codegen( app.jsonnet DEP_PKGS iomanager TEMPLATES Structs.hpp.j2 Nljs.hpp.j2 )
# daq_codegen( appinfo.jsonnet DEP_PKGS opmonlib TEMPLATES opmonlib/InfoStructs.hpp.j2 opmonlib/InfoNljs.hpp.j2 )

# See https://dune-daq-sw.readthedocs.io/en/latest/packages/daq-cmake/#daq_protobuf_codegen

daq_protobuf_codegen(opmon/*.proto)

daq_add_dal_library(datafilter.schema.xml NAMESPACE dunedaq::datafilter::dal DALDIR dal DEP_PKGS confmodel)

##############################################################################
# Main library
# daq_add_library (*.cpp LINK_LIBRARIES stdc++fs ers::ers HighFive hdf5libs dfmodules dfmessages::dfmessages daqdataformats::daqdataformats detdataformats::detdataformats \\
                    # fddetdataformats::fddetdataformats detchannelmaps::detchannelmaps  iomanager::iomanager opmonlib::opmonlib logging::logging nlohmann_json::nlohmann_json ${CETLIB} ${CETLIB_EXCEPT})

daq_add_library (*.cpp LINK_LIBRARIES opmonlib::opmonlib stdc++fs ers::ers HighFive hdf5libs dfmodules::dfmodules daqdataformats::daqdataformats detdataformats::detdataformats \\
                    fddetdataformats::fddetdataformats detchannelmaps::detchannelmaps iomanager::iomanager logging::logging nlohmann_json::nlohmann_json ${CETLIB} ${CETLIB_EXCEPT})
##############################################################################
# Unit tests
#daq_add_unit_test( HDF5WriteReadTriggerRecord_test LINK_LIBRARIES hdf5libs )
#daq_add_unit_test( HDF5WriteReadTimeSlice_test LINK_LIBRARIES hdf5libs )

##############################################################################
# Python bindings
# daq_add_python_bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME} hdf5libs::hdf5libs opmonlib::opmonlib dfmessages::dfmessages dfmodules::dfmodules daqdataformats::daqdataformats fddetdataformats::fddetdataformats)


##############################################################################
# See https://dune-daq-sw.readthedocs.io/en/latest/packages/daq-cmake/#daq_add_plugin
daq_add_plugin(DataFilter duneDAQModule LINK_LIBRARIES  dfmessages::dfmessages datafilter datafilter_dal appfwk::appfwk hdf5libs::hdf5libs) # The appfwk library is needed for DAQModule base class
daq_add_plugin(TRDispatcher duneDAQModule LINK_LIBRARIES  dfmessages::dfmessages datafilter datafilter_dal appfwk::appfwk hdf5libs::hdf5libs) # The appfwk library is needed for DAQModule base class
daq_add_plugin(FilterOrchestrator duneDAQModule LINK_LIBRARIES  datafilter datafilter_dal appfwk::appfwk) # The appfwk library is needed for DAQModule base class
daq_add_plugin(FilterResultWriter duneDAQModule LINK_LIBRARIES  datafilter datafilter_dal appfwk::appfwk) # The appfwk library is needed for DAQModule base class
daq_add_plugin(DummyModule2    duneDAQModule LINK_LIBRARIES  datafilter appfwk::appfwk  )

##############################################################################
# Applications
# daq_add_application(hdf5libs_datafilter_writer_test hdf5libs_datafilter_writer_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(datafilter_tr_send_test datafilter_tr_send_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter_tr_send_test-modified2 datafilter_tr_send_test-modified2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(datafilter_tr_receive_test datafilter_tr_receive_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(datafilter_tr_receive_test3 datafilter_tr_receive_test-modified2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter_tr_receive_writer datafilter_tr_receive_writer.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(dataflow_emu ../../apps/dataflow_emu.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(dataflow_emu2 ../../apps/dataflow_emu2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(datafilter2 ../../apps/datafilter2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(get_header_size get_header_size.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(dataflow_emu3 ../../apps/dataflow_emu3.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter3 ../../apps/datafilter3.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(dataflow_emu4 ../../apps/dataflow_emu4.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter4 ../../apps/datafilter4.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(iom_send_test iom_send_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(iom_receive_test iom_receive_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(iom_receive_test2 iom_receive_test2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(queues_vs_threads_iomanager queues_vs_threads_iomanager.cxx TEST LINK_LIBRARIES ${PROJECT_NAME})
#
#
#daq_add_application(datafilter_config_test ../../apps/datafilter_config_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})

daq_add_application(trdispatcher trdispatcher.cxx LINK_LIBRARIES  appfwk::appfwk okssystem::okssystem conffwk::conffwk iomanager::iomanager dfmessages::dfmessages rcif::rcif hdf5libs::hdf5libs logging::logging nlohmann_json::nlohmann_json) 
daq_add_application(filterorchestrator filterorchestrator.cxx LINK_LIBRARIES  appfwk::appfwk okssystem::okssystem conffwk::conffwk iomanager::iomanager hdf5libs::hdf5libs logging::logging nlohmann_json::nlohmann_json) 
daq_add_application(filterresultwriter filterresultwriter.cxx LINK_LIBRARIES  appfwk::appfwk okssystem::okssystem conffwk::conffwk iomanager::iomanager dfmessages::dfmessages rcif::rcif hdf5libs::hdf5libs logging::logging nlohmann_json::nlohmann_json) 
daq_add_application(datafilter2 datafilter.cxx LINK_LIBRARIES  appfwk::appfwk okssystem::okssystem conffwk::conffwk iomanager::iomanager dfmessages::dfmessages rcif::rcif hdf5libs::hdf5libs logging::logging nlohmann_json::nlohmann_json) 
daq_add_application(test_oks test_oks.cxx LINK_LIBRARIES appfwk::appfwk okssystem::okssystem conffwk::conffwk iomanager::iomanager dfmessages::dfmessages rcif::rcif hdf5libs::hdf5libs logging::logging nlohmann_json::nlohmann_json) 

daq_install()
