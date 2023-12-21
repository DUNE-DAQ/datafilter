INSTALL_DIR=/lcg/storage19/test-area

source `realpath /cvmfs/dunedaq.opensciencegrid.org/spack-externals/spack-installation/share/spack/setup-env.sh`
spack load python@3.8.3%gcc@8.2.0
source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh

#v4 

setup_dbt fddaq-v4.2.0
dbt-create -c fddaq-v4.2.0 $INSTALL_DIR/dune-v4-spack
cd $INSTALL_DIR/dune-v4-spack/sourcecode

git clone https://github.com/DUNE-DAQ/hdf5libs.git
git clone https://github.com/DUNE-DAQ/detchannelmaps.git
git clone https://github.com/DUNE-DAQ/detdataformats.git
git clone https://github.com/DUNE-DAQ/serialization.git
git clone https://github.com/DUNE-DAQ/fddetdataformats.git -b fddaq-v4.2.0
git clone https://github.com/DUNE-DAQ/iomanager.git 
git clone https://github.com/DUNE-DAQ/ipm.git
git clone https://github.com/DUNE-DAQ/utilities.git
git clone https://github.com/DUNE-DAQ/datafilter.git  -b develop

cd datafilter
git clone https://github.com/pybind/pybind11_json.git

cd ../..
source dbt-env.sh
dbt-workarea-env
dbt-build
