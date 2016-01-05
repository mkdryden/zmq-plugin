import sys


def verify_tornado():
    '''
    Verify that `tornado` package is installed.

    The `tornado` module is not included in the `install_requires` list because
    it is not required for basic usage of the package without a tornado
    run-loop.
    '''
    try:
        import tornado
    except:
        print >> sys.stderr, 'Package `tornado` is required.  Try `pip '\
            'install tornado`.'
        raise SystemExit
