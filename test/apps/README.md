* For old V4

https://github.com/DUNE-DAQ/datafilter/tree/dunedaq-v4.1.1/

https://github.com/wchen2013a/dfbackend/tree/dunedaq-v4.1.1

* Data Filter V5 with OKS and FrameWork

* Full integration test


This suppose that you are already run the Data Filter setup script.
Open 4 terminals and run the following apps (trdispatcher, filterorchestrator,
filterresultwriter, datafilter2) in a separate terminal.

* First provide the following inputs variables in the dfSession.data.xml to TRDispatcher object.
  * is_from_storage, the default value is true.
  * storage_pathname, where are the HDF5 files
  * json_file, for storing a list of files already transfered. The default value is hdf5_files_list.json

* The output directory and file prefix odir and output_h5_filename to FilterResultWriter object

* IP Address can be changed from the inputs and outputs objects of the TRDipatcher, FilterOrchastrator,
  FilterResultWriter and DataFilter with the address attribute. The default address is 127.0.0.1.

```

cd test/apps # in each terminal

# run Data Filter on np04-srv-004 for TR dataset from a directory. The start order is not important.

#terminal 1
trdispatcher
#terminal 2
filterorchestrator
#terminal 3
datafilter2
#terminal 4
filterresultwriter

```
