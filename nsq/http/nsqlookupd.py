'''A class for interacting with a nsqlookupd instance over http'''

from . import BaseClient, json_wrap, ok_check


class Client(BaseClient):
    '''A client for talking to nsqlookupd over http'''
    @ok_check
    def ping(self):
        '''Ping the client'''
        return self.get('ping')

    @json_wrap
    def info(self):
        '''Get info about this instance'''
        return self.get('info')

    @json_wrap
    def lookup(self, topic):
        '''Look up which hosts serve a particular topic'''
        return self.get('lookup', params={'topic': topic})

    @json_wrap
    def topics(self):
        '''Get a list of topics'''
        return self.get('topics')

    @json_wrap
    def channels(self, topic):
        '''Get a list of channels for a given topic'''
        return self.get('channels', params={'topic': topic})

    @json_wrap
    def nodes(self):
        '''Get information about nodes'''
        return self.get('nodes')

    @json_wrap
    def delete_topic(self, topic):
        '''Delete a topic'''
        return self.get('delete_topic', params={'topic': topic})

    @json_wrap
    def delete_channel(self, topic, channel):
        '''Delete a channel in the provided topic'''
        return self.get('delete_channel',
            params={'topic': topic, 'channel': channel})

    @json_wrap
    def tombstone_topic_producer(self, topic, node):
        '''It's not clear what this endpoint does'''
        return self.get('tombstone_topic_producer',
            params={'topic': topic, 'node': node})

    @json_wrap
    def create_topic(self, topic):
        '''Create a topic'''
        return self.get('create_topic', params={'topic': topic})

    @json_wrap
    def create_channel(self, topic, channel):
        '''Create a channel in the provided topic'''
        return self.get('create_channel',
            params={'topic': topic, 'channel': channel})

    @json_wrap
    def debug(self):
        '''Get debugging information'''
        return self.get('debug')
