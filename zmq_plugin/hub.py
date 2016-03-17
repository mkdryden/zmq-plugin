# coding: utf-8
from collections import OrderedDict
import inspect
import itertools
import json
import logging
import re

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
        self.query_socket.send(message)

    def on_execute__register(self, request):
        source = request['header']['source']
        # Add name of client to registry.
        self.registry[source] = source
        self.logger.debug('Added "%s" to registry', source)
        # Respond with registry contents.
        return self.registry

    def on_execute__ping(self, request):
        return 'pong'

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
        try:
            # Decode message from first (and only expected) frame.
            request = json.loads(msg_frames[0])
            # Validate message against schema.
            validate(request)
        except jsonschema.ValidationError:
            self.logger.error('unexpected request', exc_info=True)
            self.reset_query_socket()

        try:
            self.publish_socket.send_multipart(map(str,
                                                   [request['header']['source'],
                                                    request['header']['target'],
                                                    request['header']
                                                    ['msg_type'],
                                                    msg_frames[0]]))
            message_type = request['header']['msg_type']
            if message_type == 'connect_request':
                reply = self._process__connect_request(request)
            elif message_type == 'execute_request':
                reply = self._process__execute_request(request)
            else:
                raise RuntimeError('Unrecognized message type: %s' %
                                   message_type)
            reply['header']['source'] = self.name
            reply_json = json.dumps(reply)
            self.query_send(reply_json)
            self.publish_socket.send_multipart(map(str,
                                                   [reply['header']['source'],
                                                    reply['header']['target'],
                                                    reply['header']
                                                    ['msg_type'], reply_json]))
        except:
            self.logger.error('Error processing request.', exc_info=True)
            self.reset_query_socket()

    def on_command_recv(self, msg_frames):
        '''
        Process multi-part message from *command* socket.

        Only `execute_request` and `execute_reply` messages are expected.

        Messages are expected under the following scenarios:

         1. A plugin submitting an execution request or reply to another
            plugin.
         2. A plugin submitting an execution request or reply to the **hub**.

        In case 1, the `source` and `target` in the message header **MUST**
        both be present in the local registry (i.e., `self.registry`).

        In case 2, the `source` in the message header **MUST** be present in
        the local registry (i.e., `self.registry`) and the `target` **MUST** be
        equal to `self.name`.

        This method may, for example, be called asynchronously as a callback in
        run loop through a `ZMQStream(...)` configuration.  See [here][1] for
        more details.

        Args:

            msg_frames (list) : Multi-part ZeroMQ message.

        Returns:

            None

        [1]: http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/multisocket/tornadoeventloop.html
        '''
        try:
            source, null, message_str = msg_frames
        except:
            self.logger.error('Unexpected message', exc_info=True)
            return

        try:
            # Decode message from first (and only expected) frame.
            message = json.loads(message_str, encoding='utf-8')
            # Validate message against schema.
            validate(message)
        except jsonschema.ValidationError:
            self.logger.error('Unexpected message', exc_info=True)
            return
        except UnicodeDecodeError:
            import pdb; pdb.set_trace()
            return

        # Message has been validated.  Verify message source matches header.
        try:
            if not message['header']['source'] == source:
                raise NameError('Message source (%s) does not header source '
                                'field (%s).' % (source,
                                                 message['header']['source']))
        except:
            self.logger.error('Source mismatch.', exc_info=True)
            return

        # Determine whether target is another plugin or the **hub** and process
        # message accordingly.
        target = message['header']['target']
        if source in self.registry and target in self.registry:
            # Both *source* and *target* are present in the local registry.
            # Forward message to *target* plugin.
            self._process__forwarding_command_message(message)
        elif (source in self.registry and target == self.name):
            # Message *source* is in the local registry and *target* is
            # **hub**.
            self._process__local_command_message(message)
        else:
            error_msg = ('Unsupported source(%s)/target(%s) '
                         'configuration.  Either source and target both '
                         'present in the local registry, or the source '
                         '**MUST** be a plugin in the local registry and '
                         'the target **MUST** be the **hub**.' % (source,
                                                                  target))
            logger.info(error_msg)
            if ((message['header']['msg_type'] == 'execute_request') and
                    not message['content'].get('silent')):
                # Send error response to source of execution request.
                reply = get_execute_reply(message,
                                          self.execute_reply_id.next(),
                                          error=IndexError(error_msg))
                self._send_command_message(reply)

    def _send_command_message(self, message):
        '''
        Serialize message to json and send to target over command socket.

        Args:

            message (dict) : Message to send.

        Returns:

            (str) : Message serialized as json.  Can be used, for example, to
                broadcast message over publish socket.
        '''
        message_json = json.dumps(message)
        msg_frames = map(str, [message['header']['target'], '', message_json])
        self.command_socket.send_multipart(msg_frames)
        return message_json

    def _process__forwarding_command_message(self, message):
        '''
        Process validated message from *command* socket, which is addressed
        from one plugin to another.

        In addition to forwarding the message to the *target* plugin through
        the *command* socket, the message *MUST* be published to the *publish*
        socket.

        Args:

            message (dict) : Message to forward to *target*.

        Returns:

            None
        '''
        message_json = self._send_command_message(message)
        if 'content' in message and not message['content'].get('silent'):
            msg_frames = [message['header']['source'],
                          message['header']['target'],
                          message['header']['msg_type'], message_json]
            self.publish_socket.send_multipart(map(str, msg_frames))

    def _process__local_command_message(self, message):
        '''
        Process validated message from *command* socket, where the **hub** is
        either the *source* or the *target* (not both).

        In addition to sending reply to the *target* plugin through the
        *command* socket, the message *MUST* be published to the *publish*
        socket.

        Args:

            message (dict) : Message to forward to *target*.

        Returns:

            None
        '''
        message_json = json.dumps(message)
        if 'content' in message and not message['content'].get('silent'):
            msg_frames = [message['header']['source'],
                          message['header']['target'],
                          message['header']['msg_type'], message_json]
            self.publish_socket.send_multipart(map(str, msg_frames))
        message_type = message['header']['msg_type']
        if message_type == 'execute_request':
            reply = self._process__execute_request(message)
            reply_json = self._send_command_message(reply)
            if not message['content'].get('silent'):
                msg_frames = [reply['header']['source'],
                              reply['header']['target'],
                              reply['header']['msg_type'], reply_json]
                self.publish_socket.send_multipart(map(str, msg_frames))
        elif message_type == 'execute_reply':
            self._process__execute_reply(message)
        else:
            self.logger.error('Unrecognized message type: %s', message_type)

    def _process__connect_request(self, request):
        '''
        Process validated `connect_request` message, where the source field of
        the header is used to add the plugin to the registry.

        Args:

            request (dict) : `connect_request` message

        Returns:

            (dict) : `connect_reply` message.
        '''
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
        return validate(reply)

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

            request (dict) : `execute_request` message

        Returns:

            (dict) : `execute_reply` message
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
                result = func(request)
                reply = get_execute_reply(request,
                                          self.execute_reply_id.next(),
                                          data=result)
            return validate(reply)
        except (Exception, ), exception:
            return get_execute_reply(request, self.execute_reply_id.next(),
                                     error=exception)

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
            session = reply['header']['session']
            if session in self.callbacks:
                # A callback was registered for the corresponding request.
                # Call callback with reply.
                func = self.callbacks[session]
                func(reply)
            else:
                # No callback registered for session.
                pass
        except:
            self.logger.error('Processing error.', exc_info=True)
