'''Contstants for NSQ'''

# NSQ Magic
MAGIC_V2 = b'  V2'

# The newline character
NL = b'\n'

# Response
FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2

# Command names
IDENTIFY = b'IDENTIFY'
AUTH = b'AUTH'
SUB = b'SUB'
PUB = b'PUB'
MPUB = b'MPUB'
RDY = b'RDY'
FIN = b'FIN'
REQ = b'REQ'
TOUCH = b'TOUCH'
CLS = b'CLS'
NOP = b'NOP'

# Heartbeat text
HEARTBEAT = b'_heartbeat_'
