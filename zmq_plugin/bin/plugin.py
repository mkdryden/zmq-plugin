# coding: utf-8
from multiprocessing import Process
import logging
import sys

from zmq_plugin.bin import verify_tornado
verify_tornado()
import zmq
from zmq.eventloop import ioloop, zmqstream

logger = logging.getLogger(__name__)


def run_plugin(task, log_level=None):
    if log_level is not None:
        logging.basicConfig(level=log_level)

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


def run_plugin_process(uri, name, subscribe_options, log_level):
    from ..plugin import Plugin

    plugin_process = Process(target=run_plugin,
                             args=(Plugin(name, uri, subscribe_options),
                                   log_level))
    plugin_process.daemon = False
    plugin_process.start()


def parse_args(args=None):
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    if args is None:
        args = sys.argv

    parser = ArgumentParser(description='ZeroMQ Plugin process.')
    log_levels = ('critical', 'error', 'warning', 'info', 'debug', 'notset')
    parser.add_argument('-l', '--log-level', type=str, choices=log_levels,
                        default='info')
    parser.add_argument('-s', '--subscribe-opts', type=str, default=None)
    parser.add_argument('hub_uri')
    parser.add_argument('name', type=str)

    args = parser.parse_args()
    args.log_level = getattr(logging, args.log_level.upper())
    if args.subscribe_opts is not None:
        subscribe_opts = dict([[v.strip() for v in
                                args.subscribe_opts.split(':')]
                               for kv in args.subscribe_opts.split(',')])
        args.subscribe_opts = dict([(getattr(zmq, k), v)
                                    for k, v in subscribe_opts.items()])
    return args


if __name__ == '__main__':
    args = parse_args()

    logging.basicConfig(level=args.log_level)
    run_plugin_process(args.hub_uri, args.name, args.subscribe_opts,
                       args.log_level)
