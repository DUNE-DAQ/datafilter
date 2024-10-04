#!/bin/bash


if [[ $SHELL == *"bash" ]]; then
    continue
else
    echo "You are running $SHELL. You need bash shell to continue the installation."
    return 0
fi    

hn=`hostname -s`
if [[ $hn == *"np02"* || $hn == *"np04"*  ]]; then
        echo " setup datafilter for np02 or np04"
        source ~np04daq/bin/web_proxy.sh
        cd $HOME
        mkdir -p test-area
        INSTALL_DIR=$HOME/test-area/dune-v4-spack-datafilter-integration-test
else
        INSTALL_DIR=/lcg/storage19/test-area/dune-v4-spack-datafilter-integration-test
fi

DUNE_DAQ_release=NFD_PROD4_240929_A9 


if [ -d $HOME/test-area ]; then
        source `realpath /cvmfs/dunedaq.opensciencegrid.org/spack-externals/spack-installation/share/spack/setup-env.sh`
        source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh

        #v4 
        setup_dbt latest
        dbt-create -n $DUNE_DAQ_release $INSTALL_DIR/ 
        cd $INSTALL_DIR
        source env.sh
        cd sourcecode
        
        git clone https://github.com/DUNE-DAQ/daqconf.git -b production/v4
        git clone https://github.com/DUNE-DAQ/daqsystemtest.git -b production/v4
        git clone https://github.com/DUNE-DAQ/dfmodules.git -b production/v4
        git clone https://github.com/DUNE-DAQ/fddaqconf.git -b production/v4
        
        cd daqconf ; git checkout 397b444b78; cd ..
        cd daqsystemtest ; git checkout 695dfa94c56; cd ..
        cd dfmodules ; git checkout bd61f366c; cd ..
        cd fddaqconf ; git checkout 0575bb1ade; cd ..
        
        git clone https://github.com/DUNE-DAQ/hdf5libs.git -b fddaq-v4.1.0
        git clone https://github.com/DUNE-DAQ/detchannelmaps.git
        git clone https://github.com/DUNE-DAQ/detdataformats.git
        git clone https://github.com/DUNE-DAQ/fddetdataformats.git 
        git clone https://github.com/DUNE-DAQ/datafilter.git  -b develop
        git clone https://github.com/wchen2013a/dfbackend.git
        
        cd datafilter
        git clone https://github.com/pybind/pybind11_json.git
        
        cd ../..
        source dbt-env.sh
        dbt-workarea-env
        dbt-build
fi
