#!/bin/bash

if [[ $# -ne 1 ]]; then
    echo "You need to provide the debug levels from 0 to 55"
    return 0
fi

export  DUNEDAQ_ERS_DEBUG_LEVEL=$1

