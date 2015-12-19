# coding: utf-8
from collections import OrderedDict
from pprint import pformat
import inspect
import itertools
import json
import logging
import re

import jsonschema
import zmq

from .schema import (validate, get_connect_request, get_execute_request,
                     get_execute_reply, decode_content_data)

# Create module-level logger.
logger = logging.getLogger(__name__)


class Plugin(object):
    def __init__(self, name, query_uri):
        '''
        Plugin which can be connected to a network of other plugin instances
        through a central **hub**.

        ## Thread-safety ##

        All socket configuration, registration, etc. is performed *only* when
        the `reset` method is called explicitly.  Thus, all sockets are created
        in the thread that calls the `reset` method.

        By creating sockets in the thread the calls `reset`, it is
        straightforward to, for example, run a `Plugin` in a separate process
        or thread.

        Args:

            name (str) : Unique name across all plugins.
            query_uri (str) : The URI address of the **hub** query socket.
        '''
        self.name = name

        host_cre = re.compile(r'^(?P<transport>[^:]+)://(?P<host>[^:]+)(:(?P<port>\d+)?)')

        match = host_cre.search(query_uri)
        self.transport = match.group('transport')
        self.host = match.group('host')

        self.query_uri = query_uri
        self.query_socket = None
        self.command_socket = None
        self.subscribe_socket = None
        self.execute_reply_id = itertools.count(1)

        # Registry of functions to call upon receiving `execute_reply`
        # messages, keyed by the `session_id` field of the
        # `execute_request`/`execute_reply` header.
        self.callbacks = OrderedDict()

    @property
    def logger(self):
        '''
        Return logger configured with a name in the following form:

            <module_name>.<class_name>.<method_name>->"<self.name>"
        '''
        return logging.getLogger('.'.join((__name__, str(type(self).__name__),
                                           inspect.stack()[1][3]))
                                 + '->"%s"' % self.name)

    def reset(self):
        '''
        Reset the plugin state.

        This includes:

          - Resetting the execute reply identifier counter.
          - Resetting the `command`, `query`, and `publish` sockets.
          - Registering with the central **hub**.
        '''
        self.execute_reply_id = itertools.count(1)

        self.reset_query_socket()
        connect_request = get_connect_request(self.name, 'hub')
        reply = self.query(connect_request)
        self.hub_socket_info = reply['content']
        self.reset_subscribe_socket()
        self.reset_command_socket()
        self.register()

    def register(self):
        '''
        Register as a plugin with the central **hub**.

        Registration also updates the local plugin registry, which contains the
        name of all plugins registered with the **hub** at the time of
        registration.

        Note that this method is safe to execute multiple times.  This provides
        a mechanism to refresh the local plugin registry.
        '''
        connect_request = get_execute_request(self.name, 'hub', 'register')
        reply = self.query(connect_request)
        self.plugin_registry = decode_content_data(reply)
        self.logger.info('Registered with hub at "%s"', self.query_uri)

    def reset_query_socket(self):
        '''
        Create and configure *query* socket (existing socket is destroyed if it
        exists).
        '''
        context = zmq.Context.instance()

        if self.query_socket is not None:
            self.query_socket = None

        self.query_socket = zmq.Socket(context, zmq.REQ)
        self.query_socket.connect(self.query_uri)

    def reset_command_socket(self):
        '''
        Create and configure *command* socket (existing socket is destroyed if
        it exists).
        '''
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
        '''
        Create and configure *subscribe* socket (existing socket is destroyed
        if it exists).
        '''
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
        '''
        Send request message to **hub**, receive response, and return decoded
        reply message.

        Args:

            request (dict) : `<...>_request` message.

        Returns:

            None
        '''
        try:
            self.query_socket.send(json.dumps(request))
            reply = json.loads(self.query_socket.recv(**kwargs))
            validate(reply)
            return reply
        except:
            self.logger.error('Query error', exc_info=True)
            self.reset_query_socket()
            raise

    def execute(self, target_name, command, callback=None, **kwargs):
        '''
        Send request to execute the specified command to the identified target.

        Args:

            target_name (str) : Name (i.e., ZeroMQ identity) of the target.
            command (str) : Name of command to execute.
            callback (function) : Function to call on received response.
                Callback signature is `callback_func(reply)`, where `reply` is
                an `execute_reply` message.
            **kwargs (dict) : Keyword arguments for command.

        Returns:

            None
        '''
        request = get_execute_request(self.name, target_name, command,
                                      data=kwargs)
        self.command_socket.send_multipart(['hub', '', json.dumps(request)])

    def on_command_recv(self, frames):
        '''
        Process multi-part message from command socket.

        This method may, for example, be called asynchronously as a callback in
        run loop through a `ZMQStream(...)` configuration.  See [here][1] for
        more details.

        Args:

            frames (list) : Multi-part ZeroMQ message.

        Returns:

            None

        [1]: http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/multisocket/tornadoeventloop.html
        '''
        try:
            message_str = frames[-1]
            message = json.loads(message_str)
            validate(message)
        except jsonschema.ValidationError:
            self.logger.error('unexpected message', exc_info=True)

        self.logger.debug('in %s', frames)
        message_type = message['header']['msg_type']
        if message_type == 'execute_request':
            self._process__execute_request(message)
        elif message_type == 'execute_reply':
            self._process__execute_reply(message)
        else:
            self.logger.error('Unrecognized message type: %s', message_type)

    def _process__execute_reply(self, reply):
        '''
        Process validated `execute_reply` message.

        If a callback function was registered during the execution request call
        the callback function on the reply message.

        Args:

            reply (dict) : `execute_reply` message

        Returns:

            None
        '''
        try:
            session_id = reply['header']['session_id']
            if session_id in self.callbacks:
                self.logger.debug('Calling callback for session: %s',
                                  session_id)
                # A callback was registered for the corresponding request.
                # Call callback with reply.
                func = self.callbacks[session_id]
                func(reply)
            else:
                self.logger.warning('No callback registered for session: %s',
                                    session_id)
        except:
            self.logger.error('Processing error.', exc_info=True)

    def _process__execute_request(self, request):
        '''
        Process validated `execute_request` message, which includes the name of
        the command to execute.

        If a method with the name `on_execute__<command>` exists, call the
        method on the `request` and send the return value wrapped in an
        `execute_reply` message to the source of the request.

        If the no matching method exists or if an exception is encountered
        while processing the command, send `execute_reply` message with
        corresponding error information to the source of the request.

        Args:

            reply (dict) : `execute_request` message

        Returns:

            None
        '''
        try:
            func = getattr(self, 'on_execute__' +
                           request['content']['command'], None)
            if func is None:
                error = NameError('Unrecognized command: %s' %
                                  request['content']['command'])
                reply = get_execute_reply(request,
                                          self.execute_reply_id.next(),
                                          error=error)
            else:
                reply = func(request)
            validate(reply)
            reply_str = json.dumps(reply)
        except (Exception, ), exception:
            reply = get_execute_reply(request, self.execute_reply_id.next(),
                                      error=exception)
            reply_str = json.dumps(reply)

        self.command_socket.send_multipart(['hub', '', reply_str])

    def command_recv(self, *args, **kwargs):
        '''
        Execute a read on the command socket (`args` and `kwargs` passed
        through to `recv_multipart` call).
        '''
        frames = self.command_socket.recv_multipart(*args, **kwargs)
        return frames

    def on_subscribe_recv(self, msg_frames):
        '''
        Process multi-part message from subscribe socket.

        This method may, for example, be called asynchronously as a callback in
        run loop through a `ZMQStream(...)` configuration.  See [here][1] for
        more details.

        Args:

            frames (list) : Multi-part ZeroMQ message.

        Returns:

            None

        [1]: http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/multisocket/tornadoeventloop.html
        '''
        import cPickle as pickle

        try:
            logger.info(pformat(pickle.loads(msg_frames[0])))
        except:
            logger.error('Deserialization error', exc_info=True)
