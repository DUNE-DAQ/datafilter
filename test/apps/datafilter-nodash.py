"""
Data filter process; receive TR and write it to file.

test_type = 2 # hdf5libs else h5py.

"""
# from dunedaq
import hdf5libs
import detchannelmaps
import detdataformats
import daqdataformats

import time
import timeit
import datetime
import numpy as np
import zmq
import socket
import sys

import h5py
import json
#import xjson

from rich import print

import click

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80)) #google DNS
    return s.getsockname()[0]

def create_socket(zmq_port: str="5556", topicfilter: str="data") -> zmq.Socket:
    context = zmq.Context()
    zmq_socket = context.socket(zmq.SUB)
    zmq_socket.connect ("tcp://localhost:%s" % zmq_port)
    zmq_socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
    return zmq_socket

def recv_from_dataflow(topic: str='data') -> np.ndarray:
    with create_socket() as socket:
        topic = socket.recv_string()
        data = socket.recv_pyobj()
        socket.close()
    return data


def receive_h5py_dset(ofilename):
    context = zmq.Context()
    # create the subscriber socket
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5556")
    subscriber.setsockopt(zmq.SUBSCRIBE, b'data')
    
    syncclient = context.socket(zmq.REQ)
    syncclient.connect("tcp://localhost:5562")
    
    # send a synchronization request
    syncclient.send(b'')
    print(f'Waiting for data from dataflow emulator.')
    # wait for synchronization reply
    syncclient.recv()
   
    # receive the message attributes
    message_data = recv_from_dataflow()
    
    attrs=message_data['attrs']
    record_header_dataset=message_data['record_header_dataset']
    n_frames = message_data['n_frames']
    dd = message_data['dd']
    block_size=message_data['block_size']
    #print(dd,len(dd))
    # using h5py
    with h5py.File(ofilename, "w") as f1:
       for attr in attrs:
         f1.attrs[attr[0]]=attr[1]
         
    with h5py.File(ofilename, "a") as f2:
        print(f'Start to write {record_header_dataset} to file')
        if record_header_dataset not in f2.keys():
           dset=f2.create_dataset(record_header_dataset,(block_size*n_frames,1),dtype=np.int8)
           for i in range(n_frames):
               dset[i:i+block_size]=dd[i]
           f2.flush()
    
        while True:
            # send a synchronization request
            syncclient.send(b'')
            # wait for synchronization reply
            msg=syncclient.recv()
            if msg == b"stop":
                break
            start_time = timeit.default_timer()
            topic = subscriber.recv_string()
            message_data = subscriber.recv_pyobj()
            #message_data = recv_from_dataflow()
            record_header_dataset=message_data['record_header_dataset']
            n_frames = message_data['n_frames']
            #print("===>n_frames",n_frames, "data_size \n",len(data))
            print("record_header_dataset",record_header_dataset)
            dd = message_data['dd']
            if record_header_dataset not in f2.keys():
               dset=f2.create_dataset(record_header_dataset,(block_size*n_frames,1),dtype=np.int8)
               for i in range(n_frames):
                  dset[i:i+block_size,0]=dd[i]
               f2.flush()
            write_time = timeit.default_timer()-start_time
            print(f'It takes {write_time:2.4f} s to write {n_frames} frames of {record_header_dataset}')

        print(f'Done')
        sys.exit(0)
def receive_hdf5libs_dset(ofilename):
    context = zmq.Context()
    # create the subscriber socket to sync between DF emulator and datafilter
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5556")
    subscriber.setsockopt(zmq.SUBSCRIBE, b'data')
    
    syncclient = context.socket(zmq.REQ)
    syncclient.connect("tcp://localhost:5562")
    
    # send a synchronization request
    syncclient.send(b'')
    # wait for synchronization reply
    syncclient.recv()
   
    # receive the message attributes
    message_data = recv_from_dataflow()
    
    attrs = message_data['attrs']
    attrs_keys = [item for item in attrs]
    attrs_dict = dict(attrs_keys)
    run_number = attrs_dict['run_number'] 
    app_name=attrs_dict['application_name']
    fl_conf=attrs_dict['filelayout_params']
    file_index=attrs_dict['file_index']

    print(fl_conf)
    #fl_conf_nl = xjson.dumps(fl_conf)

#    with open("testwriter_conf.json", "w") as ofile:
#        json.dump(fl_conf, ofile)

    print(type(attrs_dict),attrs_dict,run_number)
    while True:
        syncclient.send(b'')
        # wait for synchronization reply
        msg=syncclient.recv()
        if msg == b"stop":
            break
        start_time = timeit.default_timer()
        topic = subscriber.recv_string()
        message_data = subscriber.recv_pyobj()

        record_header_dataset=message_data['record_header_dataset']
        n_frames = message_data['n_frames']
        dd = message_data['adcs']
        ts = message_data['ts']
        block_size=message_data['n_frames']
        print(f'======================={"="*30}\n')
        print(f'{record_header_dataset}')
        print(dd,len(dd))
        data = [str(x) for x in dd]
        #ofile_name ="/opt/tmp/chen/testwriter2.hdf5"
        hdf5libs.data_writer2(ofilename, run_number, file_index, app_name, fl_conf,data)
    print(f'Done')
    sys.exit(0)

@click.command()
@click.argument('ofilename', type=click.Path(exists=False))
@click.option('--test_type', default=1, help='test_type : 1 ->h5py or 2 -> hdf5libs')
def main(ofilename,test_type):

    if test_type == 2:
       receive_hdf5libs_dset(ofilename)
    else:
       receive_h5py_dset(ofilename)

if __name__ == '__main__':
    main()
