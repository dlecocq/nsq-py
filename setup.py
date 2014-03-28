#! /usr/bin/env python
import sys
from nsq import __version__

extra = {}

try:
    from setuptools import setup
    if sys.version_info >= (3,):
        extra['use_2to3'] = True
except ImportError:
    from distutils.core import setup


setup(name               = 'nsq-py',
    version              = __version__,
    description          = 'NSQ for Python With Pure Sockets',
    url                  = 'http://github.com/dlecocq/nsq-py',
    author               = 'Dan Lecocq',
    author_email         = 'dan@moz.com',
    license              = "MIT License",
    keywords             = 'nsq, queue',
    packages             = ['nsq', 'nsq.http', 'nsq.sockets'],
    package_dir          = {'nsq': 'nsq', 'nsq.http': 'nsq/http', 'nsq.sockets': 'nsq/sockets'},
    classifiers          = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent'
    ],
    **extra
)
