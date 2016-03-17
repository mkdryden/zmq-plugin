import base64
import cPickle as pickle
import copy
import json
import uuid

import arrow
import jsonschema
import yaml


# ZeroMQ Plugin message format as [json-schema][1] (inspired by
# [IPython messaging format][2]).
#
# See [here][3] for information on content transfer encodings.
#
# [1]: https://python-jsonschema.readthedocs.org/en/latest/
# [2]: http://jupyter-client.readthedocs.org/en/latest/messaging.html#messaging
# [3]: https://www.w3.org/Protocols/rfc1341/5_Content-Transfer-Encoding.html
MESSAGE_SCHEMA = {
    'definitions':
    {'unique_id': {'type': 'string', 'description': 'Typically UUID'},
     'header' :
     {'type': 'object',
      'properties':
      {'msg_id': {'$ref': '#/definitions/unique_id',
                  'description':
                  'Typically UUID, should be unique per message'},
       'session' :  {'$ref': '#/definitions/unique_id',
                     'description':
                     'Typically UUID, should be unique per session'},
       'date': {'type': 'string',
                'description':
                'ISO 8601 timestamp for when the message is created'},
       'source': {'type': 'string',
                  'description': 'Name/identifier of message source (unique '
                  'across all plugins)'},
       'target': {'type': 'string',
                  'description': 'Name/identifier of message target (unique '
                  'across all plugins)'},
       'msg_type' : {'type': 'string',
                     'enum': ['connect_request', 'connect_reply',
                              'execute_request', 'execute_reply'],
                     'description': 'All recognized message type strings.'},
       'version' : {'type': 'string',
                    'default': '0.5',
                    'enum': ['0.2', '0.3', '0.4', '0.5'],
                    'description': 'The message protocol version'}},
      'required': ['msg_id', 'session', 'date', 'source', 'target', 'msg_type',
                   'version']},
     'base_message':
     {'description': 'ZeroMQ Plugin message format as json-schema (inspired '
      'by IPython messaging format)',
      'type': 'object',
      'properties':
      {'header': {'$ref': '#/definitions/header'},
       'parent_header':
       {'description':
        'In a chain of messages, the header from the parent is copied so that '
        'clients can track where messages come from.',
        '$ref': '#/definitions/header'},
       'metadata': {'type': 'object',
                    'description': 'Any metadata associated with the message.',
                    'properties': {'transfer_encoding':
                                   {'type': 'string',
                                    'default': '8bit'}}},
       'content': {'type': 'object',
                   'description': 'The actual content of the message must be a '
                   'dict, whose structure depends on the message type.'}},
      'required': ['header']},
    'execute_request':
    {'description': 'Request to perform an execution request.',
     'allOf': [{'$ref': '#/definitions/base_message'},
               {'properties':
                {'content':
                 {'type': 'object',
                  'properties':
                  {'command': {'description':
                               'Command to be executed by the target',
                               'type': 'string'},
                   'data': {'description': 'The execution arguments.'},
                   'metadata': {'type': 'object',
                                'description': 'Contains any metadata that '
                                'describes the output.'},
                   'silent': {'type': 'boolean',
                              'description': 'A boolean flag which, if True, '
                              'signals the plugin to execute this code as '
                              'quietly as possible. silent=True will *not* '
                              'broadcast output on the IOPUB channel.',
                              'default': False},
                   'stop_on_error':
                   {'type': 'boolean',
                    'description': 'A boolean flag, which, if True, does not '
                    'abort the execution queue, if an exception is '
                    'encountered. This allows the queued execution of multiple'
                    ' execute_requests, even if they generate exceptions.',
                    'default': False}},
                  'required': ['command']}}}]},
    'error':
    {'properties':
     {'ename': {'type': 'string',
                'description': "Exception name, as a string"},
      'evalue': {'type': 'string',
                 'description': "Exception value, as a string"},
      'traceback': {"type": "array",
                    'description':
                    "The traceback will contain a list of frames, represented "
                    "each as a string."}},
     'required': ['ename']},
    'execute_reply':
    {'description': 'Response from an execution request.',
     'allOf': [{'$ref': '#/definitions/base_message'},
               {'properties':
                {'content':
                 {'type': 'object',
                  'properties':
                  {'command': {'description': 'Command executed',
                               'type': 'string'},
                   'status': {'type': 'string',
                              'enum': ['ok', 'error', 'abort']},
                   'execution_count':
                   {'type': 'number',
                    'description': 'The execution counter that increases by one'
                    ' with each request.'},
                   'data': {'description': 'The execution result.'},
                   'metadata': {'type': 'object',
                                'description': 'Contains any metadata that '
                                'describes the output.'},
                   'silent': {'type': 'boolean',
                              'description': 'A boolean flag which, if True, '
                              'signals the plugin to execute this code as '
                              'quietly as possible. silent=True will *not* '
                              'broadcast output on the IOPUB channel.',
                              'default': False},
                   'error': {'$ref': '#/definitions/error'}},
                  'required': ['command', 'status', 'execution_count']}}}],
     'required': ['content']},
    'connect_request':
    {'description': 'Request to get basic information about the plugin hub, '
     'such as the ports the other ZeroMQ sockets are listening on.',
     'allOf': [{'$ref': '#/definitions/base_message'}]},
    'connect_reply':
    {'description': 'Basic information about the plugin hub.',
     'allOf': [{'$ref': '#/definitions/base_message'},
               {'properties':
                {'content':
                 {'type': 'object',
                  'properties':
                  {'command': {'type': 'object',
                               'properties': {'uri': {'type': 'string'},
                                              'port': {'type': 'number'},
                                              'name': {'type': 'string'}},
                               'required': ['uri', 'port', 'name']},
                   'publish': {'type': 'object',
                               'properties': {'uri': {'type': 'string'},
                                              'port': {'type': 'number'}},
                               'required': ['uri', 'port']}},
                  'required': ['command', 'publish']}}}],
     'required': ['content', 'parent_header']}
    },
}


def get_schema(definition):
    schema = copy.deepcopy(MESSAGE_SCHEMA)
    schema['allOf'] = [{'$ref': '#/definitions/%s' % definition}]
    return schema


message_types = (['base_message'] + MESSAGE_SCHEMA['definitions']['header']
                 ['properties']['msg_type']['enum'])
MESSAGE_SCHEMAS = dict([(k, get_schema(k)) for k in message_types])

# Pre-construct a validator for each message type.
MESSAGE_VALIDATORS = dict([(k, jsonschema.Draft4Validator(v))
                           for k, v in MESSAGE_SCHEMAS.iteritems()])


def validate(message):
    '''
    Validate message against message types defined in `MESSAGE_SCHEMA`.

    Args:

        message (dict) : One of the message types defined in `MESSAGE_SCHEMA`.

    Returns:

        (dict) : Message.  A `jsonschema.ValidationError` is raised if
            validation fails.
    '''
    MESSAGE_VALIDATORS['base_message'].validate(message)

    # Message validated as a basic message.  Now validate as specific type.
    msg_type = message['header']['msg_type']
    MESSAGE_VALIDATORS[msg_type].validate(message)
    return message


def decode_content_data(message):
    '''
    Validate message and decode data from content according to mime-type.

    Args:

        message (dict) : One of the message types defined in `MESSAGE_SCHEMA`.

    Returns:

        (object) : Return deserialized object from `content['data']` field of
            message.  A `RuntimeError` is raised if `content['error']` field is
            set.
    '''
    validate(message)

    error = message['content'].get('error', None)
    if error is not None:
        raise RuntimeError(error)

    mime_type = 'application/python-pickle'
    transfer_encoding = 'BASE64'
    metadata = message['content'].get('metadata', None)
    if metadata is not None:
        mime_type = metadata.get('mime_type', mime_type)
        transfer_encoding = metadata.get('transfer_encoding',
                                         transfer_encoding)

    data = message['content'].get('data', None)

    if data is None:
        return None

    # If content data was base64 encoded, decode it.
    #
    # [1]: https://www.w3.org/Protocols/rfc1341/5_Content-Transfer-Encoding.html
    if transfer_encoding == 'BASE64':
        data = base64.b64decode(data)

    if mime_type == 'application/python-pickle':
        # Pickle object.
        return pickle.loads(data)
    elif mime_type == 'application/x-yaml':
        return yaml.loads(data)
    elif mime_type == 'application/json':
        return json.loads(data)
    elif mime_type in ('application/octet-stream', 'text/plain'):
        return data
    else:
        raise ValueError('Unrecognized mime-type: %s' % mime_type)


def encode_content_data(data, mime_type='application/python-pickle',
                        transfer_encoding='BASE64'):
    content = {}

    if data is not None:
        if mime_type == 'application/python-pickle':
            # Pickle object.
            content['data'] = pickle.dumps(data, protocol=-1)
        elif mime_type == 'application/x-yaml':
            content['data'] = yaml.dumps(data)
        elif mime_type is None or mime_type in ('application/octet-stream',
                                                'application/json',
                                                'text/plain'):
            content['data'] = data

        # Encode content data as base64, if necessary.
        #
        # [1]: https://www.w3.org/Protocols/rfc1341/5_Content-Transfer-Encoding.html
        if transfer_encoding == 'BASE64':
            content['data'] = base64.b64encode(content['data'])

        if mime_type is not None:
            content['metadata'] = {'mime_type': mime_type}
    return content


def get_header(source, target, message_type, session=None):
    return {'msg_id': str(uuid.uuid4()),
            'session' : session or str(uuid.uuid4()),
            'date': arrow.now().isoformat(),
            'source': source,
            'target': target,
            'msg_type': message_type,
            'version': '0.4'}


def get_connect_request(source, target):
    '''
    Construct a `connect_request` message.

    Args:

        source (str) : Source name/ZMQ identifier.
        target (str) : Target name/ZMQ identifier.

    Returns:

        (dict) : A `connect_request` message.
    '''
    header = get_header(source, target, 'connect_request')
    return {'header': header}


def get_connect_reply(request, content):
    '''
    Construct a `connect_reply` message.

    Args:

        request (dict) : The `connect_request` message corresponding to the
            reply.
        content (dict) : The content of the reply.

    Returns:

        (dict) : A `connect_reply` message.
    '''
    header = get_header(request['header']['target'],
                        request['header']['source'],
                        'connect_reply',
                        session=request['header']['session'])
    return {'header': header,
            'parent_header': request['header'],
            'content': content}


def get_execute_request(source, target, command, data=None,
                        mime_type='application/python-pickle',
                        transfer_encoding='BASE64', silent=False,
                        stop_on_error=False):
    '''
    Construct an `execute_request` message.

    Args:

        source (str) : Source name/ZMQ identifier.
        target (str) : Target name/ZMQ identifier.
        command (str) : Name of command to execute.
        data (dict) : Keyword arguments to command.
        mime_type (dict) : Mime-type of requested data serialization format.
            By default, data is serialized using `pickle`.
        silent (bool) : A boolean flag which, if `True`, signals the plugin to
            execute this code as quietly as possible. If `silent=True`, reply
            will *not* broadcast output on the IOPUB channel.
        stop_on_error (bool) : A boolean flag, which, if `True`, does not abort
            the execution queue, if an exception is encountered. This allows
            the queued execution of multiple `execute_request` messages, even
            if they generate exceptions.

    Returns:

        (dict) : An `execute_request` message.
    '''
    header = get_header(source, target, 'execute_request')
    content = {'command': command, 'silent': silent,
               'stop_on_error': stop_on_error}
    content.update(encode_content_data(data, mime_type=mime_type,
                                       transfer_encoding=transfer_encoding))
    return {'header': header, 'content': content}


def get_execute_reply(request, execution_count, status='ok', error=None,
                      data=None, mime_type='application/python-pickle',
                      transfer_encoding='BASE64', silent=None):
    '''
    Construct an `execute_reply` message.

    Args:

        request (dict) : The `execute_request` message corresponding to the
            reply.
        execution_count (int) : The number execution requests processed by
            plugin, including the request corresponding to the reply.
        status (str) : One of `'ok', 'error', 'abort'`.
        error (exception) : Exception encountered during processing of request
            (if applicable).
        data (dict) : Result data.
        mime_type (dict) : Mime-type of requested data serialization format.
            By default, data is serialized using `pickle`.
        silent (bool) : A boolean flag which, if `True`, signals the plugin to
            execute this code as quietly as possible. If `silent=True`, reply
            will *not* broadcast output on the IOPUB channel.  If `None`,
            silent setting from request will be used.

    Returns:

        (dict) : An `execute_reply` message.
    '''
    header = get_header(request['header']['target'],
                        request['header']['source'],
                        'execute_reply',
                        session=request['header']['session'])
    if status == 'error' and error is None:
        raise ValueError('If status is "error", `error` must be provided.')
    content = {'execution_count': execution_count,
               'status': status,
               'command': request['content']['command'],
               'silent': request['content'].get('silent')
               if silent is None else silent}
    content.update(encode_content_data(data, mime_type=mime_type,
                                       transfer_encoding=transfer_encoding))

    if error is not None:
        content['error'] = str(error)
    return {'header': header,
            'parent_header': request['header'],
            'content': content}
