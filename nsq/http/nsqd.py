'''A class for interacting with a nsqd instance over http'''

from . import BaseClient, json_wrap, ok_check, ClientException
from ..util import pack


class Client(BaseClient):
    @ok_check
    def ping(self):
        '''Ping the client'''
        return self.get('ping')

    @json_wrap
    def info(self):
        '''Get information about the client'''
        return self.get('info')

    @ok_check
    def pub(self, topic, message):
        '''Publish a message to a topic'''
        return self.post('pub', params={'topic': topic}, data=message)

    @ok_check
    def mpub(self, topic, messages, binary=True):
        '''Send multiple messages to a topic. Optionally pack the messages'''
        if binary:
            # Pack and ship the data
            return self.post('mpub', data=pack(messages)[4:],
                params={'topic': topic, 'binary': True})
        elif any(b'\n' in m for m in messages):
            # If any of the messages has a newline, then you must use the binary
            # calling format
            raise ClientException(
                'Use `binary` flag in mpub for messages with newlines')
        else:
            return self.post(
                '/mpub', params={'topic': topic}, data=b'\n'.join(messages))

    @json_wrap
    def create_topic(self, topic):
        '''Create the provided topic'''
        return self.get('create_topic', params={'topic': topic})

    @json_wrap
    def empty_topic(self, topic):
        '''Empty the provided topic'''
        return self.get('empty_topic', params={'topic': topic})

    @json_wrap
    def delete_topic(self, topic):
        '''Delete the provided topic'''
        return self.get('delete_topic', params={'topic': topic})

    @json_wrap
    def pause_topic(self, topic):
        '''Pause the provided topic'''
        return self.get('pause_topic', params={'topic': topic})

    @json_wrap
    def unpause_topic(self, topic):
        '''Unpause the provided topic'''
        return self.get('unpause_topic', params={'topic': topic})

    @json_wrap
    def create_channel(self, topic, channel):
        '''Create the channel in the provided topic'''
        return self.get(
            '/create_channel', params={'topic': topic, 'channel': channel})

    @json_wrap
    def empty_channel(self, topic, channel):
        '''Empty the channel in the provided topic'''
        return self.get(
            '/empty_channel', params={'topic': topic, 'channel': channel})

    @json_wrap
    def delete_channel(self, topic, channel):
        '''Delete the channel in the provided topic'''
        return self.get(
            '/delete_channel', params={'topic': topic, 'channel': channel})

    @json_wrap
    def pause_channel(self, topic, channel):
        '''Pause the channel in the provided topic'''
        return self.get(
            '/pause_channel', params={'topic': topic, 'channel': channel})

    @json_wrap
    def unpause_channel(self, topic, channel):
        '''Unpause the channel in the provided topic'''
        return self.get(
            '/unpause_channel', params={'topic': topic, 'channel': channel})

    @json_wrap
    def stats(self):
        '''Get stats about the server'''
        return self.get('stats', params={'format': 'json'})

    def clean_stats(self):
        '''Stats with topics and channels keyed on topic and channel names'''
        stats = self.stats()
        if 'topics' in stats:  # pragma: no branch
            topics = stats['topics']
            topics = dict((t.pop('topic_name'), t) for t in topics)
            for topic, data in topics.items():
                if 'channels' in data:  # pragma: no branch
                    channels = data['channels']
                    channels = dict(
                        (c.pop('channel_name'), c) for c in channels)
                    data['channels'] = channels
            stats['topics'] = topics
        return stats
