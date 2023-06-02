INSTALL_DIR=/lcg/storage19/test-area

source `realpath /cvmfs/dunedaq.opensciencegrid.org/spack-externals/spack-installation/share/spack/setup-env.sh`
spack load python@3.8.3%gcc@8.2.0
source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh

#v2 
setup_dbt dunedaq-v2.11.1  
dbt-create -c dunedaq-v2.11.1-c7 $INSTALL_DIR/dune-v2-spack-test
cd $INSTALL_DIR/dune-v2-spack-test/sourcecode

git clone https://github.com/DUNE-DAQ/hdf5libs.git -b dunedaq-v2.11.1
git clone https://github.com/DUNE-DAQ/detchannelmaps.git -b dunedaq-v2.11.1
git clone https://github.com/DUNE-DAQ/detdataformats.git -b dunedaq-v2.11.1
git clone https://github.com/DUNE-DAQ/datafilter.git  -b test
cd datafilter
git https://github.com/pybind/pybind11_json.git

cd ..
source dbt-env.sh
dbt-workarea-env
#cd source/hdf5libs
#git checkout dunedaq-v2.11.1
dbt-build
