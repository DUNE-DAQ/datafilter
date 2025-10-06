#!/bin/bash


if [[ $SHELL == *"bash" ]]; then
    	true
else
	echo "You are running $SHELL. You need bash shell to continue the installation."
        return 0
fi    

hn=`hostname -s`
if [[ $hn == *"np02"* || $hn == *"np04"*  ]]; then
        echo "setup datafilter for np02 or np04"
        source ~np04daq/bin/web_proxy.sh
    	cd $HOME
        mkdir -p test-area
        INSTALL_DIR=$HOME/test-area/dune-v5-spack-datafilter-integration-test
else
        HOME=/lcg/storage19
        INSTALL_DIR=/lcg/storage19/test-area/dune-v5-spack-datafilter-integration-test6
fi

# we take the second last tag 
#NFD_PROD5_V=`ls -d /cvmfs/dunedaq-development.opensciencegrid.org/nightly/NFD_PROD4_*|sort|tail -2|head -1|cut -f5 -d "/"`
# temporary use the fddaq-v5.3.2-rc3
NFD_PROD5_V="fddaq-v5.3.2-rc3-a9"
DUNE_DAQ_release=$NFD_PROD5_V 
echo $DUNE_DAQ_release

if [ -d /lcg/storage19/test-area ]; then
        source `realpath /cvmfs/dunedaq.opensciencegrid.org/spack-externals/spack-installation/share/spack/setup-env.sh`
        source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh

        #v5 
        setup_dbt latest
        dbt-create -b candidate $DUNE_DAQ_release $INSTALL_DIR/ 
	if [ -d $INSTALL_DIR ]; then
                cd $INSTALL_DIR
                source env.sh
                cd sourcecode
                
                git clone https://github.com/DUNE-DAQ/daqsystemtest.git 
                git clone https://github.com/DUNE-DAQ/fddaqconf.git -b coredaq-v5.4.3
               
                git clone https://github.com/DUNE-DAQ/hdf5libs.git -b develop
                git clone https://github.com/DUNE-DAQ/detdataformats.git
                git clone https://github.com/DUNE-DAQ/fddetdataformats.git 
                git clone https://github.com/DUNE-DAQ/datafilter.git  -b develop
                git clone https://github.com/DUNE-DAQ/appfwk.git
                
                cd appfwk
                git checkout 1f8ce77ccdf23b8e068b8096218cfb26dd87d80e
                cd ..
                cd hdf5libs
                git checkout e1dc1f0b19bd64dbae5a36f5f525d70d4de4f6cb
                cd ..
                cd fddetdataformats
                git checkout 240f71fe8391a7d04db048f111e182ee8313de34

                cd datafilter
                
                cd ../..
                source dbt-env.sh
                dbt-workarea-env
                dbt-build -j$(nproc)
	else
		echo "$INSTALL_DIR does not exist."
	fi
fi
