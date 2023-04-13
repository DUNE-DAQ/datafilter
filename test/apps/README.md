* Start the data flow emulator first in one terminal

```
python3 dataflow_emu.py np04_coldbox_run014182_0000_dataflow0_20220712T102315.hdf5

```
* Start the datafilter process in another terminal

```
python3 datafilter-nodash.py output.hdf5
 
```

* Fast test. Modify the variable *is_full_write* to "0" in dataflow_emu.py.
