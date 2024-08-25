"""
Data flow emulator.

test1 : send all dataset
test2 : send all dataset with "0" inserted to selected Link from TR.
test3 : send all hdf5libs dataset

"""
import sys

import daqdataformats
import fddetdataformats
from rawdatautils.unpack.wib import *

from hdf5libs import HDF5RawDataFile
import h5py

import numpy as np
import zmq

import time
import timeit
import datetime

from rich import print

import click

#initial setup condition
is_full_write = 0   #1: full; 0: 1% of data will be sent to the datafilter for testing

def print_usage():
    print("Usage: dataflow_emu.py <input_h5file_name>")

def create_socket(zmq_port: str="5556", topicfilter: str="data") -> zmq.Socket:
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUB)
    zmq_socket.sndhwm = 1100000
    zmq_socket.bind("tcp://*:%s" % zmq_port)
    return zmq_socket

def send_h5py_dset_test1(ifilename):
    context = zmq.Context()
    socket=create_socket()

    socket_sync = context.socket(zmq.REP)
    socket_sync.bind("tcp://*:5562")

    SUBSCRIBERS_EXPECTED = 1
    subscribers = 0
    while subscribers < SUBSCRIBERS_EXPECTED:
        mesg=socket_sync.recv()
        socket_sync.send(b'')
        subscribers +=1
        print(f"+1 subscriber ({subscribers}/{SUBSCRIBERS_EXPECTED})")

    f=get_dataset_from_file(ifilename)

    dset_keys = get_dataset_keys(f)
    attrs_items=[item for item in f.attrs.items()]

    for trh_x in dset_keys:
       start_time=timeit.default_timer()
       trh=trh_x.rsplit('/',1)[0]
       trh_link=trh_x.rsplit('/',1)[1]
       dset=f[trh]
       data1=dset.get(trh_link)
       #data=[str(x) for x in data1[0:10]]
       #data=[str(x) for x in data1]
       record_header_dataset = trh_x
       data_size=len(data1)

       if is_full_write:
           dd=data1[0:data_size]  #all data
       else:
           if data_size < 10000:
              dd=data1[0:data_size]  #all data
           else:
              dd=data1[0:int(data_size*0.01)]  #1% of data

       n_frames = len(dd) 
       read_time=timeit.default_timer() - start_time
       print(f"Read all {trh_x} entries take {read_time} s")
       print(f'Sending {n_frames} of {data_size} for {trh_x}')

       start_time=timeit.default_timer()

       message_data={'attrs' : attrs_items, 'record_header_dataset': record_header_dataset, 'n_frames' : n_frames, 'dd' : dd, "block_size":1}
       topic="data"

       socket.send_string(topic, zmq.SNDMORE)
       socket.send_pyobj(message_data)
       #time.sleep(1)
       mesg=socket_sync.recv()
       socket_sync.send(b'')


    mesg=socket_sync.recv()
    socket_sync.send(b'stop')
    sys.exit(0)

def send_h5py_dset_test2(ifilename):
    global is_full_write
    context = zmq.Context()
    socket=create_socket()
    socket_sync = context.socket(zmq.REP)
    socket_sync.bind("tcp://*:5562")

    SUBSCRIBERS_EXPECTED = 1
    subscribers = 0
    while subscribers < SUBSCRIBERS_EXPECTED:
        mesg=socket_sync.recv()
        socket_sync.send(b'')
        subscribers +=1
        print(f"+1 subscriber ({subscribers}/{SUBSCRIBERS_EXPECTED})")

    f=get_dataset_from_file(ifilename)

    dset_keys = get_dataset_keys(f)
    attrs_items=[item for item in f.attrs.items()]

    for trh_x in dset_keys:
       start_time=timeit.default_timer()
       trh=trh_x.rsplit('/',1)[0]
       trh_link=trh_x.rsplit('/',1)[1]
       dset=f[trh]
       data1=dset.get(trh_link)
       #data=[str(x) for x in data1[0:10]]
       #data=[str(x) for x in data1]
       record_header_dataset = trh_x
       data_size=len(data1)

       if is_full_write:
           dd=data1[0:data_size]  #all data
       else:
           if data_size < 10000:
              dd=data1[0:data_size]  #all data
           else:
              dd=data1[0:int(data_size*0.01)]  #1% of data

       link_x = record_header_dataset.rsplit('/',1)[1] 
       links_rm = {"Link10"}
       if link_x in links_rm:
          print(f"Replaced {link_x} element by zero")
          dd[dd != 0]=0

#       trigger_rm = {"Trigger"}
#       if link_x in trigger_rm:
#           print(f"Replaced {link_x} element by zero")
#           dd[ dd !=0 ] = 0
#
       n_frames = len(dd) 
       read_time=timeit.default_timer() - start_time
       print(f"Read all {trh_x} entries take {read_time:2.4f} s")
       print(f'Sending {n_frames} of {data_size} for {trh_x}')

       start_time=timeit.default_timer()

       message_data={'attrs' : attrs_items, 'record_header_dataset': record_header_dataset, 'n_frames' : n_frames, 'dd' : dd, 'block_size':1}
       topic="data"
       socket.send_string(topic, zmq.SNDMORE)
       socket.send_pyobj(message_data)
       #time.sleep(1)
       mesg=socket_sync.recv()
       socket_sync.send(b'')

    mesg=socket_sync.recv()
    socket_sync.send(b'stop')
    print(f'Finished sending data. CTRL-C in the datafilter terminal if it is not exist by itself.')
    sys.exit(0)

def send_hdf5libs_dset_test(ifilename, ofilename):
    context = zmq.Context()
    socket=create_socket()
    socket_sync = context.socket(zmq.REP)
    socket_sync.bind("tcp://*:5562")

    SUBSCRIBERS_EXPECTED = 1
    subscribers = 0
    while subscribers < SUBSCRIBERS_EXPECTED:
        mesg=socket_sync.recv()
        socket_sync.send(b'')
        subscribers +=1
        print(f"+1 subscriber ({subscribers}/{SUBSCRIBERS_EXPECTED})")

    # get attributes using h5py
    h5py_file = h5py.File(ifilename, 'r')

    h5_file = HDF5RawDataFile(ifilename)
    # get type of record
    record_type = h5_file.get_record_type()
    records = h5_file.get_all_record_ids()

    records_size=len(records)
    nrecords_to_process= 1
    print(f'records_size : {records_size}')

    for rid in records[:nrecords_to_process]:
        #get record header datasets
        record_header_dataset = h5_file.get_record_header_dataset_path(rid)
        trh = h5_file.get_trh(rid)
        #wib_geo_ids = h5_file.get_geo_ids(records[0],daqdataformats.GeoID.SystemType.kTPC)
        print(f"{record_header_dataset}: {trh.get_trigger_number()},{trh.get_sequence_number()},{trh.get_trigger_timestamp()}")

        #loop through fragment datasets
        #for gid in h5_file.get_geo_ids(rid)[:nrecords_to_process]:
        for gid in list(h5_file.get_geo_ids(rid))[:nrecords_to_process]:
            frag = h5_file.get_frag(rid,gid)
            frag_hdr=frag.get_header()
            wf = fddetdataformats.WIBFrame(frag.get_data())
            n_frames = (frag.get_size()-frag_hdr.sizeof())//fddetdataformats.WIBFrame.sizeof()
            ts = np.zeros(n_frames,dtype='uint64')
            adcs = np.zeros((n_frames,256),dtype='uint16')
            t0 = time.time()
            #send frame by frame
            for iframe in range(n_frames):
                wf = fddetdataformats.WIBFrame(frag.get_data(iframe*fddetdataformats.WIBFrame.sizeof()))
                ts[iframe] = wf.get_timestamp()
                adcs[iframe] = [ wf.get_channel(k) for k in range(256) ]
                #print(f"iframe={iframe} adcs value={adcs[iframe]}")
                attrs_items=[item for item in h5py_file.attrs.items()]
                # frame by frame
                message_data={'attrs' : attrs_items, 'record_header_dataset': record_header_dataset, 'iframe' : iframe, \
                        'n_frames' : n_frames, 'adcs' : adcs[iframe], 'ts' : ts[iframe], 'ofilename' : ofilename}
                topic="data"
                socket.send_string(topic, zmq.SNDMORE)
                socket.send_pyobj(message_data)
                time.sleep(1)
                mesg=socket_sync.recv()
                socket_sync.send(b'')

# all TR at once
#    attrs_items=[item for item in h5py_file.attrs.items()]
#    message_data={'attrs' : attrs_items, 'record_header_dataset': record_header_dataset, 'n_frames' : n_frames, 'adcs' : adcs}
#    topic="data"
#    socket.send_string(topic, zmq.SNDMORE)
#    socket.send_pyobj(message_data)
#    mesg=socket_sync.recv()
#    socket_sync.send(b'')
#    sleep(1)


def get_dataset_keys(f):
    keys = []
    f.visit(lambda key : keys.append(key) if isinstance(f[key], h5py.Dataset) else None)
    return keys

def get_dataset_from_file(ifilename):
    f=h5py.File(ifilename,'r')
    return f

@click.command()
@click.argument('ifilename', type=click.Path(exists=True))
@click.option('--test', default=1, help='test : 1 or 2' or '3')
def main(ifilename,test):

    if test == 2 : 
       send_h5py_dset_test2(ifilename)
    if test == 3:
       ofilename = "test_output.hdf5"
       print(f'output hdf5 file is {ofilename}')
       send_hdf5libs_dset_test(ifilename,ofilename)
    else:
       send_h5py_dset_test1(ifilename)

if __name__ == "__main__":
    main()

