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


def hexify(message):
    '''Print out printable characters, but others in hex'''
    import string
    hexified = []
    for char in message:
        if (char in '\n\r \t') or (char not in string.printable):
            hexified.append('\\x%02x' % ord(char))
        else:
            hexified.append(char)
    return ''.join(hexified)


def distribute(total, objects):
    '''Generator for (count, object) tuples that distributes count evenly among
    the provided objects'''
    for index, obj in enumerate(objects):
        start = (index * total) / len(objects)
        stop = ((index + 1) * total) / len(objects)
        yield (stop - start, obj)
