cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
project(datafilter VERSION 0.1.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

include_directories(pybind11_json/include)

find_package(logging REQUIRED)
find_package(ers REQUIRED)
find_package(HighFive REQUIRED)
find_package(daqdataformats REQUIRED)
find_package(detdataformats REQUIRED)
find_package(detchannelmaps REQUIRED)
find_package(hdf5libs REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(cetlib REQUIRED)   # Uses the daq-buildtools/cmake/Findcetlib.cmake
find_package(Boost COMPONENTS iostreams unit_test_framework REQUIRED)

#find_package(pybind11_json ${pybind11_json_REQUIRED_VERSION} REQUIRED)
include_directories(src/exts/nlohmann_json/include)

daq_codegen( *.jsonnet TEMPLATES Structs.hpp.j2 Nljs.hpp.j2 )

##############################################################################
# Main library
daq_add_library (*.cpp LINK_LIBRARIES stdc++fs ers::ers HighFive hdf5libs daqdataformats::daqdataformats detdataformats::detdataformats detchannelmaps::detchannelmaps logging::logging nlohmann_json::nlohmann_json ${CETLIB} ${CETLIB_EXCEPT})

##############################################################################
# Unit tests
#daq_add_unit_test( HDF5WriteReadTriggerRecord_test LINK_LIBRARIES hdf5libs )
#daq_add_unit_test( HDF5WriteReadTimeSlice_test LINK_LIBRARIES hdf5libs )

##############################################################################
# Python bindings
daq_add_python_bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME} hdf5libs::hdf5libs daqdataformats::daqdataformats)

##############################################################################
# Applications
#daq_add_application(HDF5LIBS_TestReader HDF5LIBS_TestReader.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
#daq_add_application(HDF5LIBS_TestWriter HDF5LIBS_TestWriter.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})
daq_add_application(hdf5libs_datawriter_test hdf5libs_datawriter_test.cpp TEST LINK_LIBRARIES ${PROJECT_NAME})

daq_install()