* Start the data flow emulator first in one terminal

```
python3 dataflow_emu.py np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5

```
* Start the datafilter process in another terminal

```
python3 datafilter-nodash.py output.hdf5
 
```

* Fast test. Modify the variable *is_full_write* to "0" in dataflow_emu.py.


* Run test with the datafilter with plots of the ADC values.

 * dataflow_emu first in one terminal
  ```
  python3 dataflow_emu.py --test 3 np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5
  
  ```
  * Run datafilter in another terminal

  ```
  python3 datafilter test_output.hdf5

  ```
 * You can view the plots from a browser pointing to http://yourip:8080, where
   yourip is the IP of your datafilter host.
