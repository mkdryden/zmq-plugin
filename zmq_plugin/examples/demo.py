from multiprocessing import Process
import logging

from zmq.eventloop import ioloop, zmqstream

logger = logging.getLogger(__name__)


def run_task(task):
    logging.basicConfig(level=logging.DEBUG)

    task.reset()

    # Register on receive callback.
    task.command_stream = zmqstream.ZMQStream(task.command_socket)
    task.command_stream.on_recv(task.on_command_recv)

    # Register on receive callback.
    task.query_stream = zmqstream.ZMQStream(task.query_socket)
    task.query_stream.on_recv(task.on_query_recv)

    try:
        ioloop.install()
        logger.info('Starting hub ioloop')
        ioloop.IOLoop.instance().start()
    except RuntimeError:
        logger.warning('IOLoop already running.')


if __name__ == '__main__':
    import time
    from ..hub import Hub
    from ..plugin import Plugin

    logging.basicConfig(level=logging.DEBUG)

    hub_process = Process(target=run_task,
                          args=(Hub('tcp://*:12345', 'hub') ,))
    hub_process.daemon = False
    hub_process.start()

    print '\n' + (72 * '=') + '\n'

    plugin_a = Plugin('plugin_a', 'tcp://localhost:12345')
    plugin_a.reset()

    plugin_b = Plugin('plugin_b', 'tcp://localhost:12345')
    plugin_b.reset()

    print '\n' + (72 * '=') + '\n'

    for i in xrange(3):
        # Send "ping" from `'plugin_b'` `'plugin_a'`
        logger.info('''Send "ping" from `'plugin_b'` `'plugin_a'`''')
        plugin_b.send_command('plugin_a', 'ping')

        logger.info('''Wait for command request to be received by `'plugin_a'`''')
        frames = plugin_a.command_recv()
        plugin_a.on_command_recv(frames)

        logger.info('''Wait for command response to be received by `'plugin_b'`''')
        frames = plugin_b.command_recv()
        plugin_b.on_command_recv(frames)

        print '\n' + (72 * '-') + '\n'

    hub_process.terminate()
