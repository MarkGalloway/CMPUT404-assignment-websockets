#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2013-2014 Abram Hindle
# Copyright (c) 2015 Mark Galloway
# Copyright (c) 2015 Tamara Bain
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import flask
from flask import Flask, request, redirect, Response
from flask_sockets import Sockets
import gevent
from gevent import queue
import time
import json
import os

app = Flask(__name__)
sockets = Sockets(app)
app.debug = True


class World:
    def __init__(self):
        self.clear()
        # we've got listeners now!
        self.listeners = list()

    def add_set_listener(self, listener):
        self.listeners.append(listener)

    def remove_set_listener(self, listener):
        self.listeners.remove(listener)

    def update(self, entity, key, value):
        entry = self.space.get(entity, dict())
        entry[key] = value
        self.space[entity] = entry
        self.update_listeners(entity)

    def set(self, entity, data):
        self.space[entity] = data
        self.update_listeners(entity)

    def update_listeners(self, entity):
        '''update the set listeners'''
        for listener in self.listeners:
            listener(entity, self.get(entity))

    def clear(self):
        self.space = dict()

    def get(self, entity):
        return self.space.get(entity, dict())

    def world(self):
        return self.space

myWorld = World()


def send_all(msg):
    for client in clients:
        client.put( msg )


def send_all_json(obj):
    send_all( json.dumps(obj) )


class Client:
    def __init__(self):
        self.queue = queue.Queue()

    def put(self, v):
        self.queue.put_nowait(v)

    def get(self):
        return self.queue.get()

clients = list()


def set_listener(entity, data):
    ''' do something with the update ! '''
    send_all_json({entity: data})


myWorld.add_set_listener(set_listener)


@app.route('/')
def hello():
    '''Return something coherent here.. perhaps redirect to /static/index.html '''
    return redirect('static/index.html')


def read_ws(ws, client=None):
    '''A greenlet function that reads from the websocket and updates the world'''
    # XXX: TODO IMPLEMENT ME
    try:
        while True:
            msg = ws.receive()

            print "WS RECV: %s" % msg

            if (msg is not None):
                packet = json.loads(msg)

                # Update the entity
                for entity in packet:
                    # print(entity)
                    for key in packet[entity]:
                        myWorld.update(entity, key, packet[entity][key])

            else:
                break
    except:
        '''Done'''


@sockets.route('/subscribe')
def subscribe_socket(ws):
    '''Fufill the websocket URL of /subscribe, every update notify the
       websocket and read updates from the websocket '''
    # XXX: TODO IMPLEMENT ME

    client = Client()
    clients.append(client)

    g = gevent.spawn(read_ws, ws, client)

    print "Subscribing"

    # Send the world
    ws.send(json.dumps(myWorld.world()))

    try:
        while True:
            # block here
            msg = client.get()
            # Got a message !
            # print("got a message!")
            ws.send(msg)

    except Exception as e: # WebSocketError as e:
        print "WS Error %s" % e
    finally:
        # ws.close()
        clients.remove(client)
        gevent.kill(g)


# JSON response builder
def build_json_response(body=None, code=200):
    return Response(response=body, status=code, mimetype="application/json")


def flask_post_json():
    '''Ah the joys of frameworks! They do so much work for you
       that they get in the way of sane operation!'''
    if (request.json is not None):
        return request.json
    elif (request.data is not None and request.data != ''):
        return json.loads(request.data)
    else:
        return json.loads(request.form.keys()[0])


@app.route("/entity/<entity>", methods=['POST', 'PUT'])
def update(entity):
    '''update the entities via this interface'''
    # Parse the JSON
    body = request.get_json(force=True)

    # Update the entity
    for key in body:
        myWorld.update(entity, key, body[key])

    # Return the entity
    data = json.dumps(myWorld.get(entity))
    return build_json_response(data)


@app.route("/world", methods=['POST', 'GET'])
def world():
    '''you should probably return the world here'''
    # Return the World as JSON
    data = json.dumps(myWorld.world())
    return build_json_response(data)


@app.route("/entity/<entity>")
def get_entity(entity):
    '''This is the GET version of the entity interface, return a representation of the entity'''
    # Return the entity as JSON
    data = json.dumps(myWorld.get(entity))
    return build_json_response(data)


@app.route("/clear", methods=['POST', 'GET'])
def clear():
    '''Clear the world out!'''
    myWorld.clear()

    # Return the world as JSON
    if request.method == 'POST':
        data = json.dumps(myWorld.world())
        return build_json_response(data)

    return Response(status=200)


if __name__ == "__main__":
    ''' This doesn't work well anymore:
        pip install gunicorn
        and run
        gunicorn -k flask_sockets.worker sockets:app
    '''
    app.run()
