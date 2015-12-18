# coding: utf-8
from pprint import pformat
import re
import logging
import itertools
from collections import OrderedDict
import json
import cPickle as pickle
import yaml

import zmq
from .schema import (validate, get_connect_request, get_execute_request,
                     get_execute_reply, decode_content_data)

logger = logging.getLogger(__name__)


class Plugin(object):
    def __init__(self, name, query_uri):
        self.name = name

        host_cre = re.compile(r'^(?P<transport>[^:]+)://(?P<host>[^:]+)(:(?P<port>\d+)?)')

        match = host_cre.search(query_uri)
        self.transport = match.group('transport')
        self.host = match.group('host')

        self.query_uri = query_uri
        self.query_socket = None
        self.command_socket = None
        self.subscribe_socket = None

        self.callbacks = OrderedDict()

    @property
    def logger(self):
        import inspect

        return logging.getLogger('.'.join((__name__, str(type(self).__name__),
                                           inspect.stack()[1][3]))
                                 + '->"%s"' % self.name)

    def reset(self):
        self.command_msg_id = itertools.count()

        self.reset_query_socket()
        connect_request = get_connect_request(self.name, 'hub')
        reply = self.query(connect_request)
        self.hub_socket_info = reply['content']
        self.reset_subscribe_socket()
        self.reset_command_socket()
        self.register()

    def register(self):
        connect_request = get_execute_request(self.name, 'hub', 'register')
        reply = self.query(connect_request)
        self.plugin_registry = decode_content_data(reply)
        self.logger.info('Registered with hub at "%s"', self.query_uri)

    def reset_query_socket(self):
        context = zmq.Context.instance()

        if self.query_socket is not None:
            self.query_socket = None

        self.query_socket = zmq.Socket(context, zmq.REQ)
        self.query_socket.connect(self.query_uri)

    def reset_command_socket(self):
        context = zmq.Context.instance()

        if self.command_socket is not None:
            self.command_socket = None

        # Create command socket and assign name as identity.
        self.command_socket = zmq.Socket(context, zmq.ROUTER)
        self.command_socket.setsockopt(zmq.IDENTITY, bytes(self.name))
        command_uri = '%s://%s:%s' % (self.transport, self.host,
                                      self.hub_socket_info['command']['port'])
        self.command_socket.connect(command_uri)
        self.logger.info('Connected command socket to "%s"', command_uri)

    def reset_subscribe_socket(self):
        context = zmq.Context.instance()

        if self.subscribe_socket is not None:
            self.subscribe_socket = None

        # Create subscribe socket and assign name as identity.
        self.subscribe_socket = zmq.Socket(context, zmq.SUB)
        self.subscribe_socket.setsockopt(zmq.SUBSCRIBE, '')
        subscribe_uri = '%s://%s:%s' % (self.transport, self.host,
                                        self.hub_socket_info['publish']
                                        ['port'])
        self.subscribe_socket.connect(subscribe_uri)
        self.logger.info('Connected subscribe socket to "%s"', subscribe_uri)

    def query(self, request, **kwargs):
        try:
            self.query_socket.send(json.dumps(request))
            reply = json.loads(self.query_socket.recv(**kwargs))
            validate(reply)
            return reply
        except:
            self.logger.error('Query error', exc_info=True)
            self.reset_query_socket()
            raise

    def send_command(self, target_name, data):
        self.command_socket.send_multipart(['hub', target_name, '',
                                            str(self.command_msg_id.next()), data])

    def on_command_recv(self, frames):
        self.logger.debug('in %s', frames)
        try:
            a, b, null, msg_id = frames[:4]
            msg = frames[4:]
            command, args = msg[0], msg[1:]
            if command == 'ping':
                out_msg = [a, b, null, msg_id, 'pong']
                self.logger.debug('out %s', out_msg)
                self.command_socket.send_multipart(out_msg)
            elif command == 'pong':
                pass
            else:
                raise ValueError('Unknown command, "%s"' % command)
        except:
            self.logger.error('Unrecognized command structure.', exc_info=True)

    def command_recv(self, *args, **kwargs):
        frames = self.command_socket.recv_multipart(*args, **kwargs)
        return frames

    def on_subscribe_recv(self, msg_frames):
        import cPickle as pickle

        try:
            logger.info(pformat(pickle.loads(msg_frames[0])))
        except:
            logger.error('Deserialization error', exc_info=True)
