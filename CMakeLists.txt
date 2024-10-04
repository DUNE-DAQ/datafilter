cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
project(datafilter VERSION 0.1.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

include_directories(pybind11_json/include)
include_directories(pybind11/include)

find_package(logging REQUIRED)
find_package(ers REQUIRED)
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
find_package(Boost COMPONENTS iostreams unit_test_framework REQUIRED)

#find_package(pybind11_json ${pybind11_json_REQUIRED_VERSION} REQUIRED)
include_directories(src/exts/nlohmann_json/include)

daq_codegen( app.jsonnet DEP_PKGS iomanager TEMPLATES Structs.hpp.j2 Nljs.hpp.j2 )
daq_codegen( appinfo.jsonnet DEP_PKGS opmonlib TEMPLATES opmonlib/InfoStructs.hpp.j2 opmonlib/InfoNljs.hpp.j2 )

##############################################################################
# Main library
daq_add_library (*.cpp LINK_LIBRARIES stdc++fs ers::ers HighFive hdf5libs dfmodules dfmessages::dfmessages daqdataformats::daqdataformats detdataformats::detdataformats \\
                    fddetdataformats::fddetdataformats detchannelmaps::detchannelmaps iomanager::iomanager logging::logging nlohmann_json::nlohmann_json ${CETLIB} ${CETLIB_EXCEPT})

##############################################################################
# Unit tests
#daq_add_unit_test( HDF5WriteReadTriggerRecord_test LINK_LIBRARIES hdf5libs )
#daq_add_unit_test( HDF5WriteReadTimeSlice_test LINK_LIBRARIES hdf5libs )

##############################################################################
# Python bindings
daq_add_python_bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME} hdf5libs::hdf5libs dfmessages::dfmessages dfmodules::dfmodules daqdataformats::daqdataformats fddetdataformats::fddetdataformats)

##############################################################################
# Applications
daq_add_application(hdf5libs_datafilter_writer_test hdf5libs_datafilter_writer_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(datafilter_tr_send_test datafilter_tr_send_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(datafilter_tr_receive_test datafilter_tr_receive_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter_tr_receive_writer datafilter_tr_receive_writer.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(dataflow_emu ../../apps/dataflow_emu.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(dataflow_emu2 ../../apps/dataflow_emu2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(datafilter2 ../../apps/datafilter2.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(dataflow_emu3 ../../apps/dataflow_emu3.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter3 ../../apps/datafilter3.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(dataflow_emu4 ../../apps/dataflow_emu4.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(datafilter4 ../../apps/datafilter4.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(iom_send_test iom_send_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(iom_receive_test iom_receive_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#
#
#daq_add_application(datafilter_config_test ../../apps/datafilter_config_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})

daq_install()
