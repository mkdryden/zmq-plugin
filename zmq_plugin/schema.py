# ZeroMQ Plugin message format as [json-schema][1] (inspired by
# [IPython messaging format][2]).
#
# [1]: https://python-jsonschema.readthedocs.org/en/latest/
# [2]: http://jupyter-client.readthedocs.org/en/latest/messaging.html#messaging

MESSAGE_SCHEMA = {
    'definitions':
    {'header' :
     {'type': 'object',
      'properties':
      {'msg_id': {'type': 'string',
                  'description': 'Typically UUID, must be unique per message'},
       'session' : {'type': 'string',
                    'description':
                    'Typically UUID, should be unique per session'},
       'date': {'type': 'string',
                'description':
                'ISO 8601 timestamp for when the message is created'},
       'msg_type' : {'type': 'string',
                     'description':
                     'All recognized message type strings are listed below.'},
       'version' : {'type': 'string',
                    'default': '0.1',
                    'enum': ['0.1'],
                    'description': 'The message protocol version'}},
      'required': ['msg_id', 'session', 'date', 'msg_type', 'version']}},

    'type': 'object',
    'properties':
    {'header': {r'$ref': '#/definitions/header'},
     # In a chain of messages, the header from the parent is copied so that
     # clients can track where messages come from.
     'parent_header': {r'$ref': '#/definitions/header'},
     'metadata': {'type': 'object',
                  'description': 'Any metadata associated with the message.'},

     'content': {'type': 'object',
                 'description': 'The actual content of the message must be a '
                 'dict, whose structure depends on the message type.'}},
    'required': ['header', 'content']}
