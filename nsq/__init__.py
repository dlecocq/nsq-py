import logging

# Logging, obviously
logger = logging.getLogger('nsq')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(filename)s@%(lineno)d: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Our underlying json implmentation
try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json

# The current version
__version__ = '0.1.10'
