from .client import Client

class Reader(Client):
    '''A reader client'''
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc, value, trace):
        pass

    def __iter__(self):
        pass
