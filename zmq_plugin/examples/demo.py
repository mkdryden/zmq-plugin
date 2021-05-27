import pprint
from multiprocessing import Process
import logging

import zmq
from zmq.eventloop import ioloop, zmqstream
from tornado.ioloop import PeriodicCallback

logger = logging.getLogger(__name__)


def run_hub(task):
    logging.basicConfig(level=logging.DEBUG)

    task.reset()

    # Register on receive callback.
    task.command_stream = zmqstream.ZMQStream(task.command_socket)
    task.command_stream.on_recv(task.on_command_recv)

    # Register on receive callback.
    task.query_stream = zmqstream.ZMQStream(task.query_socket)
    task.query_stream.on_recv(task.on_query_recv)

    def dump_registry():
        print('\n' + (72 * '*') + '\n')
        print(task.registry)
        print('\n' + (72 * '*') + '\n')

    try:
        ioloop.install()
        logger.info('Starting hub ioloop')
        PeriodicCallback(dump_registry, 100,
                         io_loop=ioloop.IOLoop.instance()).start()
        ioloop.IOLoop.instance().start()
    except RuntimeError:
        logger.warning('IOLoop already running.')


def run_plugin(task):
    logging.basicConfig(level=logging.DEBUG)

    task.reset()

    # Register on receive callback.
    task.command_stream = zmqstream.ZMQStream(task.command_socket)
    task.command_stream.on_recv(task.on_command_recv)

    # Register on receive callback.
    task.query_stream = zmqstream.ZMQStream(task.subscribe_socket)
    task.query_stream.on_recv(task.on_subscribe_recv)

    try:
        ioloop.install()
        logger.info('Starting plugin %s ioloop' % task.name)
        ioloop.IOLoop.instance().start()
    except RuntimeError:
        logger.warning('IOLoop already running.')


if __name__ == '__main__':
    import time
    from ..hub import Hub
    from ..plugin import Plugin

    logging.basicConfig(level=logging.DEBUG)

    hub_process = Process(target=run_hub,
                          args=(Hub('tcp://*:12345', 'hub') ,))
    hub_process.daemon = False
    hub_process.start()

    plugin_process = Process(target=run_plugin,
                             args=[Plugin('plugin_a',
                                          'tcp://localhost:12345')])
    plugin_process.daemon = False
    plugin_process.start()

    print('\n' + (72 * '=') + '\n')

    plugin_b = Plugin('plugin_b', 'tcp://localhost:12345')
    plugin_b.reset()

    #print '\n' + (72 * '=') + '\n'

    #for i in xrange(3):
        ## Send "ping" from `'plugin_b'` `'plugin_a'`
        #logger.info('''Send "ping" from `'plugin_b'` `'plugin_a'`''')
        #plugin_b.send_command('plugin_a', 'ping')

        #logger.info('''Wait for command response to be received by `'plugin_b'`''')
        #frames = plugin_b.command_recv()
        #plugin_b.on_command_recv(frames)

        #print '\n' + (72 * '-') + '\n'

    #print '\n# Plugin B subscribed message dump #\n'

    #while True:
        #try:
            #logger.info(pprint.pformat(plugin_b.subscribe_socket
                                       #.recv_pyobj(zmq.NOBLOCK)))
        #except zmq.Again:
            #break

    plugin_process.terminate()
    hub_process.terminate()
