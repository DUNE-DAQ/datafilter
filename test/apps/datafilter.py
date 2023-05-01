"""
Datafilter process; receive TR and write the TR to file.
using DASH.
"""
#import _daq_hdf5libs_py as hdf5libs
import hdf5libs
import detchannelmaps
import detdataformats
import daqdataformats


import time
import datetime
# import dash packages for plots
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import plotly
import plotly.graph_objects as go
import numpy as np
import zmq
import socket
import h5py
import json

#with h5py.File("testwriter.hdf5", "w") as f:
#       dset = f.create_dataset("mytestdataset",dtype="i")

# receive attributes first
isAttr=1 

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
    return data


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Datafilter plot'),

#    html.Div(children='''
#        Dash: A web application framework for Python.
#    '''),

    dcc.Graph(
        id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000, # in milliseconds
        n_intervals=0
    )
])

# The updating is done with this callback
@app.callback(
    Output('live-update-graph', 'figure'),
    [Input('interval-component', 'n_intervals')])
def update(n):
    global isAttr

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
    print(f"message_data {message_data['iframe']} {message_data['adcs']} {len(message_data['adcs'])}") 

    if isAttr==1:
        print(message_data)
        attrs=message_data['attrs']
        isAttr=0
        with h5py.File("testwriter.hdf5", "w") as f:
           dset = f.create_dataset("mytestdataset",dtype='i')
           print(attrs)
           for attr in attrs:
              print(attr[0],attr[1])
              #data1=json.dumps(attr)
              dset.attrs[attr[0]]=attr[1]
              #print("----->",data1)
    else:
        with h5py.File("testwriter.hdf5", "a") as f:
          #print(attr[0],attr[1])
          #data1=json.dumps(attr)
          print(message_data)
          #dset.group[attr[0]]=attr[1]
          #print("----->",data1)

        # update trigger records
        #hdf5libs.data_writer2(ofile_name, run_number, file_index, app_name, files,hw_map_file_name)

    #f.close()
    define_color="#330C73"
    # Create the graph with subplots
    fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)
    fig['layout']['margin'] = {
        'l': 30, 'r': 10, 'b': 30, 't': 10
    }
    fig['layout']['legend'] = {'x': 1, 'y': 1, 'xanchor': 'left'}

#    fig.append_trace({
#        'y': message_data['adcs'],
#        'name': 'adcs',
#        'mode': 'markers',
#        'type': 'scatter'
#        'marker_color' : 'define_color'
#    }, 1, 1)
#    trace1 = go.Scatter(y=message_data['adcs'], mode='markers')
    #msd=message_data['adcs']
    #print(f"message_data[adcs] {msd[0]} {len(msd[0])}")
    trace1 = go.Histogram(x=message_data['adcs'], name='ADCs accepted',nbinsx=256)
#    trace2 = go.Histogram(x=message_data['adcs'], name='ADCs accepted', nbinsx=256)
#    fig.append_trace(trace1,1,1)
    fig.append_trace(trace1,1,1)

#    fig.append_trace(trace2,2,1)
#    fig.append_trace({
#        'x': message_data['adcs'],
#        'name': 'adcs accepted',
#        'mode': 'markers',
#        'type': 'histogram'
#    }, 1, 1)
    fig.update_layout(
        xaxis_title_text='ADC Value', # xaxis label
        yaxis_title_text='Count', # yaxis label
        bargap=0.2, # gap between bars of adjacent location coordinates
        bargroupgap=0.1 # gap between bars of the same location coordinates
    )

    return fig


#while True:
#   d = recv_from_dataflow('data')
#   ts=d['timestamp']
#   secs = ts / 1e9
#   #print(d['iframe'], "  ",secs," ",  datetime.datetime.fromtimestamp(secs).strftime('%Y-%m-%d %H:%M:%S'))
#   print(d['iframe'], "  ",ts," ",d['data'])
if __name__ == '__main__':
    app.run_server(debug=True, host='10.0.0.14', port='8080')
