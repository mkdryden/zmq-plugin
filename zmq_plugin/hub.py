# coding: utf-8
from collections import OrderedDict
import re
import logging

import zmq

logger = logging.getLogger(__name__)


class Hub(object):
    def __init__(self, query_uri, name='hub'):
        host_cre = re.compile(r'^(?P<transport>[^:]+)://(?P<host>[^:]+)(:(?P<port>\d+)?)')

        match = host_cre.search(query_uri)
        self.transport = match.group('transport')
        self.host = match.group('host')

        self.name = name

        self.query_uri = query_uri
        self.query_socket = None

        # Command URI is determined at time of binding (bound to random port).
        self.command_uri = None
        self.command_socket = None
        self.registry = OrderedDict()

    def reset(self):
        self.reset_query_socket()
        self.reset_command_socket()

    def reset_query_socket(self):
        context = zmq.Context.instance()

        if self.query_socket is not None:
            self.query_socket.close()
            self.query_socket = None

        # Create command socket and assign name as identity.
        self.query_socket = zmq.Socket(context, zmq.REP)
        self.query_socket.bind(self.query_uri)

    def reset_command_socket(self):
        context = zmq.Context.instance()

        if self.command_socket is not None:
            self.command_socket.close()
            self.command_socket = None

        # Create command socket and assign name as identity.
        self.command_socket = zmq.Socket(context, zmq.ROUTER)
        self.command_socket.setsockopt(zmq.IDENTITY, bytes(self.name))
        base_uri = "%s://%s" % (self.transport, self.host)
        self.command_port = self.command_socket.bind_to_random_port(base_uri)
        self.command_uri = base_uri + (':%s' % self.command_port)

    def on_query_recv(self, msg_frames):
        try:
            request_code = msg_frames[0]
            assert(request_code in ('register', 'socket_info'))
            logger.debug('in "%s"', request_code)
            if request_code == 'register':
                name = msg_frames[1]
                # Add name of client to registry.
                self.registry[name] = name
                # Send list of registered clients.
                logger.debug('out %s', self.registry)
                self.query_socket.send_pyobj(self.registry)
            elif request_code == 'socket_info':
                # Send socket info.
                socket_info = {'command': {'uri': self.command_uri,
                                           'port': self.command_port,
                                           'name': self.name}}
                logger.debug('out %s', socket_info)
                self.query_socket.send_pyobj(socket_info)
        except:
            logger.error('unexpected message', exc_info=True)
            self.reset_query_socket()

    def on_command_recv(self, msg_frames):
        try:
            logger.debug('in "%s"', msg_frames)
            a, b, null, msg_id, msg = msg_frames
            logger.debug('out [%s]->"%s": "%s"', msg_id, b, msg)
            self.command_socket.send_multipart([b, a, null, msg_id, msg])
        except:
            logger.error('unexpected message')
