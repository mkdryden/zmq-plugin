import jsonschema


# ZeroMQ Plugin message format as [json-schema][1] (inspired by
# [IPython messaging format][2]).
#
# [1]: https://python-jsonschema.readthedocs.org/en/latest/
# [2]: http://jupyter-client.readthedocs.org/en/latest/messaging.html#messaging
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
                    'default': '0.2',
                    'enum': ['0.2'],
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
                    'description': 'Any metadata associated with the message.'},
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
                   'silent': {'type': 'boolean',
                              'description': 'A boolean flag which, if True, '
                              'signals the plugin to execute this code as '
                              'quietly as possible. silent=True will *not*: '
                              'broadcast output on the IOPUB channel, or have '
                              'an `execute_result`',
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
                  {'status': {'type': 'string',
                              'enum': ['ok', 'error', 'abort']},
                   'execution_count':
                   {'type': 'number',
                    'description': 'The execution counter that increases by one'
                    ' with each request.'}},
                  'required': ['status', 'execution_count']},
                 'error': {'$ref': '#/definitions/error'}}}],
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
                                              'name': {'type': 'string'}}},
                   'publish': {'type': 'object',
                               'properties': {'uri': {'type': 'string'},
                                              'port': {'type':
                                                       'number'}}}},
                  'required': ['command', 'publish']}}}],
     'required': ['content']}
    },
}


def get_schema(definition):
    import copy

    schema = copy.deepcopy(MESSAGE_SCHEMA)
    schema['allOf'] = [{'$ref': '#/definitions/%s' % definition}]
    return schema


message_types = (['base_message'] + MESSAGE_SCHEMA['definitions']['header']
                 ['properties']['msg_type']['enum'])
MESSAGE_SCHEMAS = dict([(k, get_schema(k)) for k in message_types])

# Pre-construct a validator for each message type.
MESSAGE_VALIDATORS = dict([(k, jsonschema.Draft4Validator(v))
                           for k, v in MESSAGE_SCHEMAS.iteritems()])


def validate(data):
    MESSAGE_VALIDATORS['base_message'].validate(data)

    # Message validated as a basic message.  Now validate as specific type.
    msg_type = data['header']['msg_type']
    MESSAGE_VALIDATORS[msg_type].validate(data)
    return data
