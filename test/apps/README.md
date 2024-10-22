* Start the data flow emulator first in one terminal

```
python3 dataflow_emu.py np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5

```
* Start the data filter process in another terminal

```
python3 datafilter-nodash.py output.hdf5
 
```

* Fast test. Modify the variable *is_full_write* to "0" in dataflow_emu.py.

* Check the results using h5diff

```
h5diff output.hdf5  np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5
 
```

See h5diff -h for more options. You can also use h5dump to dump both files to
text format and using vimdiff to view the difference.


* Run test with the data filter with plots of the ADC values.

 * dataflow_emu first in one terminal
  ```
  python3 dataflow_emu.py --test 3 np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5
  
  ```
  * Run data filter in another terminal

  ```
  pip3 install dash # if we don't have it install
  python3 datafilter.py test_output.hdf5
=======
  python3 datafilter 

  ```
 * You can view the plots from a browser pointing to http://yourip:8080, where
   yourip is the IP of your data filter host.

* Test data filter writer

  * Modify Detector_Readout_0x00000064_WIBEth in RawData

  ```

  hdf5libs_datafilter_writer_test  np04hd_run024559_0009_dataflow0_datawriter_0_20240321T103447.hdf5 test2.h5 0
  
  # to check the results
  h5diff -c test2.h5 np04hd_run024559_0009_dataflow0_datawriter_0_20240321T103447.hdf5

  ```

 * Remove Detector_Readout_0x00000064_WIBEth from RawData

 ```

  hdf5libs_datafilter_writer_test  np04hd_run024559_0009_dataflow0_datawriter_0_20240321T103447.hdf5 test2.h5 1

  # to check for the dataset Detector_Readout_0x00000064_WIBEth 

  h5dump -d /TriggerRecord00253.0000/RawData/Detector_Readout_0x00000064_WIBEth test2.h5
 ```

* Run Trigger Record tranfer test.

 * start the receiver first

```
# sender on the same node
datafilter_tr_receive_test -d `pwd` -o test

# sender on different node
datafilter_tr_receive_test --server np04-srv-004 -d `pwd` -o test
```
 * then start the sender of the a generated Trigger Record.

 ```
# sender on the same node as receiver
datafilter_tr_send_test 

# sender on different node as receiver
datafilter_tr_send_test  --server np04-srv-004
 ```
 * check the output HDF5 file 

 ```
hdf5_dump.py -f test1.hdf5 -p header
hdf5_dump.py -f test1.hdf5 -p fragment

 ```

* start to send trigger records from HDF5 file

```
datafilter_tr_send_test -f swtest_run001039_0000_dataflow0_datawriter_0_20231103T121050.hdf5 --hdf5

```

* Full integration test

Open 4 terminals and run the following apps (trdispatcher, filterorchestrator,
datafilter2, filterresultwriter) in a separate terminal.


```
# for generated simple TR
trdispatcher

# for TR dataset
trdispatcher -f np04hd_run024559_0009_dataflow0_datawriter_0_20240321T103447.hdf5 --hdf5
```

```
filterorchestrator
```

```
datafilter2
```

```
filterresultwriter -d `pwd` -o test2
```


