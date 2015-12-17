# coding: utf-8
import re
import logging
import itertools
from collections import OrderedDict

import zmq

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

        self.callbacks = OrderedDict()

    def reset(self):
        self.command_msg_id = itertools.count()

        self.reset_query_socket()
        self.plugin_registry = self.query('register', self.name)
        self.hub_socket_info = self.query('socket_info')
        self.reset_command_socket()

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

    def query(self, request_code, *args, **kwargs):
        try:
            self.query_socket.send_multipart([request_code] + list(args))
            return self.query_socket.recv_pyobj(**kwargs)
        except:
            logger.error('Query error', exc_info=True)
            self.reset_query_socket()
            raise

    def send_command(self, target_name, data, callback=None):
        self.command_socket.send_multipart(['hub', target_name, '',
                                            str(self.command_msg_id.next()), data])

    def on_command_recv(self, frames):
        logger = logging.getLogger('.'.join((__name__,
                                             str(type(self).__name__),
                                             self.name)))
        logger.debug('in %s', frames)
        try:
            a, b, null, msg_id = frames[:4]
            msg = frames[4:]
            command, args = msg[0], msg[1:]
            if command == 'ping':
                out_msg = [a, b, null, msg_id, 'pong']
                logger.debug('out %s', out_msg)
                self.command_socket.send_multipart(out_msg)
            elif command == 'pong':
                pass
            else:
                raise ValueError('Unknown command, "%s"' % command)
        except:
            logger.error('Unrecognized command structure.', exc_info=True)

    def command_recv(self, *args, **kwargs):
        frames = self.command_socket.recv_multipart(*args, **kwargs)
        return frames
