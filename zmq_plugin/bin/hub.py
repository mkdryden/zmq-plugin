# coding: utf-8
from multiprocessing import Process
import logging
import sys

from zmq_plugin.bin import verify_tornado
verify_tornado()
from zmq.eventloop import ioloop, zmqstream

logger = logging.getLogger(__name__)


def run_hub(task, log_level=None):
    if log_level is not None:
        logging.basicConfig(level=log_level)

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


def run_hub_process(uri, name, log_level):
    from ..hub import Hub

    hub_process = Process(target=run_hub, args=(Hub(uri, name), log_level))
    hub_process.daemon = False
    hub_process.start()


def parse_args(args=None):
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    if args is None:
        args = sys.argv

    parser = ArgumentParser(description='ZeroMQ Plugin hub.')
    log_levels = ('critical', 'error', 'warning', 'info', 'debug', 'notset')
    parser.add_argument('-l', '--log-level', type=str, choices=log_levels,
                        default='info')
    parser.add_argument('uri')
    parser.add_argument('name', nargs='?', type=str, default='hub')

    args = parser.parse_args()
    args.log_level = getattr(logging, args.log_level.upper())
    return args


if __name__ == '__main__':
    args = parse_args()

    logging.basicConfig(level=args.log_level)
    run_hub_process(args.uri, args.name, args.log_level)
