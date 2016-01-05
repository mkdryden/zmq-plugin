import sys

from paver.easy import task, needs, path, sh, cmdopts, options
from paver.setuputils import setup, install_distutils_tasks
from distutils.extension import Extension
from distutils.dep_util import newer

sys.path.insert(0, path('.').abspath())
import version

setup(name='zmq-plugin',
      version=version.getVersion(),
      description='A spoke-hub plugin framework, using 0MQ backend.',
      keywords='',
      author='Christian Fobel',
      author_email='christian@fobel.net',
      url='https://github.com/wheeler-microfluidics/zmq-plugin',
      license='LGPLv2.1',
      packages=['zmq_plugin'],
      # N.B., install also requires `tornado` to run `bin.hub` or `bin.plugin`
      # scripts.
      install_requires=['jsonschema', 'pyyaml', 'pyzmq'],
      # Install data listed in `MANIFEST.in`
      include_package_data=True)


@task
@needs('generate_setup', 'minilib', 'setuptools.command.sdist')
def sdist():
    """Overrides sdist to make sure that our setup.py is generated."""
    pass
