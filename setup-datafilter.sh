#!/bin/bash
#
# Usage:
#   source setup-datafilter.sh [INSTALL_DIR]
#
# INSTALL_DIR priority (highest to lowest):
#   1. Script argument:       source setup-datafilter.sh /your/path
#   2. Pre-exported env var:  export INSTALL_DIR=/your/path && source setup-datafilter.sh
#   3. Hostname-based default (np02/np04 -> $HOME/test-area/...; other -> /lcg/storage19 fallback)

if [[ $SHELL != *"bash" ]]; then
    echo "You are running $SHELL. You need bash shell to continue the installation."
    return 0
fi

# --- INSTALL_DIR ---
if [ -n "$1" ]; then
    INSTALL_DIR="$1"
elif [ -z "$INSTALL_DIR" ]; then
    hn=$(hostname -s)
    if [[ $hn == *"np02"* || $hn == *"np04"* ]]; then
        echo "setup datafilter for np02 or np04"
        source ~np04daq/bin/web_proxy.sh
        mkdir -p "$HOME/test-area"
        INSTALL_DIR="$HOME/test-area/dune-v5-spack-datafilter-integration-test"
    else
        INSTALL_DIR="/lcg/storage19/test-area/dune-v5-spack-datafilter-integration-test6"
    fi
fi

echo "INSTALL_DIR: $INSTALL_DIR"

# --- Validate parent directory ---
parent_dir=$(dirname "$INSTALL_DIR")
if [ ! -d "$parent_dir" ]; then
    echo "Error: parent directory '$parent_dir' does not exist."
    echo "Create it first, or set INSTALL_DIR to a path whose parent already exists."
    return 1
fi

# --- Release pin ---
# we take the second last tag
# NFD_PROD5_V=`ls -d /cvmfs/dunedaq-development.opensciencegrid.org/nightly/NFD_PROD4_*|sort|tail -2|head -1|cut -f5 -d "/"`
# temporary: use fddaq-v5.3.2-rc3
NFD_PROD5_V="fddaq-v5.3.2-rc3-a9"
DUNE_DAQ_release=$NFD_PROD5_V
echo "$DUNE_DAQ_release"

# --- Bootstrap DUNE DAQ build tools ---
source "$(realpath /cvmfs/dunedaq.opensciencegrid.org/spack-externals/spack-installation/share/spack/setup-env.sh)"
source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh

setup_dbt latest
dbt-create -b candidate "$DUNE_DAQ_release" "$INSTALL_DIR/"

if [ ! -d "$INSTALL_DIR" ]; then
    echo "Error: dbt-create did not create '$INSTALL_DIR'. Aborting."
    return 1
fi

cd "$INSTALL_DIR"
source env.sh
cd sourcecode

git clone https://github.com/DUNE-DAQ/daqsystemtest.git
git clone https://github.com/DUNE-DAQ/fddaqconf.git -b coredaq-v5.4.3

git clone https://github.com/DUNE-DAQ/hdf5libs.git -b develop
git clone https://github.com/DUNE-DAQ/detdataformats.git
git clone https://github.com/DUNE-DAQ/fddetdataformats.git
git clone https://github.com/DUNE-DAQ/datafilter.git -b develop
git clone https://github.com/DUNE-DAQ/appfwk.git

cd appfwk
git checkout 1f8ce77ccdf23b8e068b8096218cfb26dd87d80e
cd ..
cd hdf5libs
git checkout e1dc1f0b19bd64dbae5a36f5f525d70d4de4f6cb
cd ..
cd fddetdataformats
git checkout 240f71fe8391a7d04db048f111e182ee8313de34
cd ..

cd datafilter

cd ../..
source dbt-env.sh
dbt-workarea-env
dbt-build -j$(nproc)
