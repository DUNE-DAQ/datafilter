/**
 * @file wib.cpp
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

//#include "hdf5libs/HDF5RawDataFile.hpp"
#include "datafilter/hdf5_writer_libs.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "pybind11_json/pybind11_json.hpp"

#include <string>

//int data_writer(std::string,std::string,std::string,std::string);

namespace py = pybind11;

namespace dunedaq {
namespace datafilter {
namespace python {

//  PYBIND11_MODULE(_daq_hdf5_writer_libs_py,m)
void 
register_hdf5_writer_libs(py::module& m)
  {
      m.def("data_writer",&data_writer);
      m.def("print_usage",&print_usage);
      m.def("data_writer2",&data_writer2);
  }

} // namespace python
} // namespace hdf5libs
} // namespace dunedaq
