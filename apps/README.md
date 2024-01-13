* Start the data flow emulator first in one terminal

```
dataflow_emu2 -f swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5 -r 1 -N 2

```
* Start the data filter process in another terminal

```
datafilter2 -N 2 # to start send the TR from the emulator

datafilter2 -x 1 # to get the next TR from the emulator
 
```

