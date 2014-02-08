#! /usr/bin/env python
import sys

extra = {}

try:
    from setuptools import setup
    if sys.version_info >= (3,):
        extra['use_2to3'] = True
except ImportError:
    from distutils.core import setup


setup(name               = 'nsq-py',
    version              = '0.1.0',
    description          = 'NSQ for Python With Pure Sockets',
    url                  = 'http://github.com/dlecocq/nsq-py',
    author               = 'Dan Lecocq',
    author_email         = 'dan@moz.com',
    license              = "MIT License",
    keywords             = 'nsq, queue',
    packages             = ['nsq', 'nsq.clients'],
    package_dir          = {'nsq': 'nsq', 'nsq.clients': 'nsq/clients'},
    classifiers          = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent'
    ],
    **extra
)
