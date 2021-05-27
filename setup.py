#!/usr/bin/env python
# -*- encoding: utf-8 -*-



from setuptools import setup
import sys

sys.path.insert(0, '.')
import versioneer


setup(name='zmq-plugin',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='A spoke-hub plugin framework, using 0MQ backend.',
      keywords='',
      author='Christian Fobel',
      author_email='christian@fobel.net',
      url='https://github.com/sci-bots/zmq-plugin',
      license='LGPLv2.1',
      packages=['zmq_plugin'],
      # N.B., install also requires `tornado` to run `bin.hub` or `bin.plugin`
      # scripts.
      install_requires=['arrow>=0.7.0', 'jsonschema', 'pandas', 'pyyaml',
                        'pyzmq'],
      # Install data listed in `MANIFEST.in`
      include_package_data=True)
