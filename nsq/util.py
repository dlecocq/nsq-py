'''Some utilities used around town'''

import struct


def pack(message):
    '''Pack the provided message'''
    if isinstance(message, basestring):
        # Return
        # [ 4-byte message size ][ N-byte binary data ]
        return struct.pack('>l', len(message)) + message
    else:
        # Return
        # [ 4-byte body size ]
        # [ 4-byte num messages ]
        # [ 4-byte message #1 size ][ N-byte binary data ]
        #      ... (repeated <num_messages> times)
        return pack(
            struct.pack('>l', len(message)) + ''.join(map(pack, message)))
