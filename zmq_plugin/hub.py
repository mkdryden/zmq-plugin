# coding: utf-8
from collections import OrderedDict
import re
import logging
import json
import itertools

import zmq
import jsonschema
from .schema import validate, get_connect_reply, get_execute_reply

logger = logging.getLogger(__name__)


class Hub(object):
    def __init__(self, query_uri, name='hub'):
        '''
        Central **hub** to connect a network of plugin instances.

        ## Thread-safety ##

        All socket configuration, registration, etc. is performed *only* when
        the `reset` method is called explicitly.  Thus, all sockets are created
        in the thread that calls the `reset` method.

        By creating sockets in the thread the calls `reset`, it is
        straightforward to, for example, run a `Plugin` in a separate process
        or thread.

        Args:

            query_uri (str) : The URI address of the **hub** query socket.
                Plugins connect to the query socket to register and query
                information about other sockets.
            name (str) : Unique name across all plugins.
        '''
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
        self.publish_uri = None
        self.publish_socket = None

        # Registry of connected plugins.
        self.registry = OrderedDict()

    def reset(self):
        '''
        Reset the plugin state.

        This includes:

          - Resetting the execute reply identifier counter.
          - Resetting the `publish`, `query`, and `command` sockets.
        '''
        self.execute_reply_id = itertools.count(1)
        self.reset_publish_socket()
        self.reset_query_socket()
        self.reset_command_socket()

    def reset_query_socket(self):
        '''
        Create and configure *query* socket (existing socket is destroyed if it
        exists).
        '''
        context = zmq.Context.instance()

        if self.query_socket is not None:
            self.query_socket.close()
            self.query_socket = None

        # Create command socket and assign name as identity.
        self.query_socket = zmq.Socket(context, zmq.REP)
        self.query_socket.bind(self.query_uri)

    def reset_command_socket(self):
        '''
        Create and configure *command* socket (existing socket is destroyed if
        it exists).
        '''
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

    def reset_publish_socket(self):
        '''
        Create and configure *publish* socket (existing socket is destroyed if
        it exists).
        '''
        context = zmq.Context.instance()

        if self.publish_socket is not None:
            self.publish_socket.close()
            self.publish_socket = None

        # Create publish socket and assign name as identity.
        self.publish_socket = zmq.Socket(context, zmq.PUB)
        base_uri = "%s://%s" % (self.transport, self.host)
        self.publish_port = self.publish_socket.bind_to_random_port(base_uri)
        self.publish_uri = base_uri + (':%s' % self.publish_port)

    def query_send(self, message):
        self.publish_socket.send_pyobj({'msg_type': 'query_out', 'data': message})
        logger.debug('out %s', message)
        self.query_socket.send(message)

    def command_send(self, message):
        self.publish_socket.send_pyobj({'msg_type': 'command_out',
                                        'data': message})
        logger.debug('out %s', message)
        self.command_socket.send(message)

    def _process__execute_request(self, request):
        # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
        # TODO This handler tries to match command name from request to call a
        # TODO method.  However, this is **not** what we want if we are just
        # TODO routing a message from one plugin to another.
        # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
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
                content = func(request)
                reply = get_execute_reply(request,
                                          self.execute_reply_id.next(),
                                          **content)
            return validate(reply)
        except (Exception, ), exception:
            return get_execute_reply(request, self.execute_reply_id.next(),
                                     error=exception)

    def _process__execute_reply(self, reply):
        # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
        # TODO This handler tries to match an execute reply to a registered
        # TODO callback function. However, this is **not** what we want if we
        # TODO are just routing a message from one plugin to another.
        # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
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

    def on_execute__register(self, request):
        source = request['header']['source']
        # Add name of client to registry.
        self.registry[source] = source
        logger.debug('Added "%s" to registry', source)
        # Respond with registry contents.
        return get_execute_reply(request, self.execute_reply_id.next(),
                                 data={'result': self.registry})

    def on_command_recv(self, msg_frames):
        '''
        Process multi-part message from *command* socket.

        Only `execute_request` and `execute_reply` messages are expected.

        Messages are expected under the following scenarios:

         - `execute_request`:
             1. A plugin submitting an execution request to another plugin.
             2. A plugin submitting an execution request to the **hub**.
                 * The `source` in the message header **MUST** be present in
                   the local registry (i.e., `self.registry`) and the `target`
                   **MUST** be equal to `self.name`.

         - `execute_reply`:
             1. A plugin responding with an execution reply to another plugin.
                 * The `source` and `target` in the message header **MUST**
                   both be present in the local registry (i.e.,
                   `self.registry`).
             2. A plugin responding with an execution reply from the **hub**.
                 * The `source` in the message header **MUST** be present in
                   the local registry (i.e., `self.registry`) and the `target`
                   **MUST** be equal to `self.name`.

        In case 1 for `execute_request`/`execute_reply` messages, the `source`
        and `target` in the message header **MUST** both be present in the
        local registry (i.e., `self.registry`).

        In case 2 for `execute_request`/`execute_reply` messages, the `source`
        in the message header **MUST** be present in the local registry (i.e.,
        `self.registry`) and the `target` **MUST** be equal to `self.name`.

        This method may, for example, be called asynchronously as a callback in
        run loop through a `ZMQStream(...)` configuration.  See [here][1] for
        more details.

        Args:

            msg_frames (list) : Multi-part ZeroMQ message.

        Returns:

            None

        [1]: http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/multisocket/tornadoeventloop.html
        '''
        self.publish_socket.send_pyobj({'msg_type': 'command_in',
                                        'data': msg_frames})
        try:
            logger.debug('in "%s"', msg_frames)
            a, b, null, msg_id, msg = msg_frames
            logger.debug('out [%s]->"%s": "%s"', msg_id, b, msg)
            self.command_send([b, a, null, msg_id, msg])
        except:
            logger.error('unexpected message', exc_info=True)

    def on_query_recv(self, msg_frames):
        '''
        Process multi-part message from query socket.

        This method may, for example, be called asynchronously as a callback in
        run loop through a `ZMQStream(...)` configuration.  See [here][1] for
        more details.

        Args:

            msg_frames (list) : Multi-part ZeroMQ message.

        Returns:

            None

        [1]: http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/multisocket/tornadoeventloop.html
        '''
        # Publish raw message frames to *publish* socket.
        self.publish_socket.send_pyobj({'msg_type': 'query_in',
                                        'data': msg_frames})
        try:
            # Decode message from first (and only expected) frame.
            request = json.loads(msg_frames[0])
            # Validate message against schema.
            validate(request)
        except jsonschema.ValidationError:
            logger.error('unexpected request', exc_info=True)
            self.reset_query_socket()

        try:
            logger.debug('in "%s"', request)
            message_type = request['header']['msg_type']
            if message_type == 'connect_request':
                source = request['header']['source']
                # Add name of client to registry.
                self.registry[source] = source
                # Send list of registered clients.
                socket_info = {'command': {'uri': self.command_uri,
                                           'port': self.command_port,
                                           'name': self.name},
                               'publish': {'uri': self.publish_uri,
                                           'port': self.publish_port}}
                reply = get_connect_reply(request, content=socket_info)
                validate(reply)
            elif message_type == 'execute_request':
                reply = self._process__execute_request(request)
            else:
                raise RuntimeError('Unrecognized message type: %s' %
                                   message_type)
            self.query_send(json.dumps(reply))
        except:
            logger.error('Error processing request.', exc_info=True)
            self.reset_query_socket()
