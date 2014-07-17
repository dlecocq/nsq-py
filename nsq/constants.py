'''Contstants for NSQ'''

# NSQ Magic
MAGIC_V2 = '  V2'

# The newline character
NL = '\n'

# Response
FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2

# Command names
IDENTIFY = 'IDENTIFY'
AUTH = 'AUTH'
SUB = 'SUB'
PUB = 'PUB'
MPUB = 'MPUB'
RDY = 'RDY'
FIN = 'FIN'
REQ = 'REQ'
TOUCH = 'TOUCH'
CLS = 'CLS'
NOP = 'NOP'

# Heartbeat text
HEARTBEAT = '_heartbeat_'
