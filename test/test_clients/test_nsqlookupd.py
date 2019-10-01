import uuid

from common import HttpClientIntegrationTest


class TestNsqlookupdClient(HttpClientIntegrationTest):
    def test_ping(self):
        '''Ping the client'''
        self.assertTrue(self.nsqlookupd.ping())

    def test_info(self):
        '''Get info about the client'''
        self.assertIn('version', self.nsqlookupd.info())

    def test_lookup(self):
        '''Can look up nsqd instances for a topic'''
        self.assertIn('producers', self.nsqlookupd.lookup(self.topic))

    def test_topics(self):
        '''Can get all the topics this instance knows about'''
        self.assertIn(self.topic.decode(), self.nsqlookupd.topics()['topics'])

    def test_channels(self):
        '''Can get all the channels in the provided topic'''
        self.assertIn(self.channel.decode(),
            self.nsqlookupd.channels(self.topic)['channels'])

    def test_nodes(self):
        '''Can get information about all the nodes'''
        self.assertIn('producers', self.nsqlookupd.nodes())

    def test_delete_topic(self):
        '''Can delete topics'''
        self.nsqlookupd.delete_topic(self.topic)
        self.assertNotIn(self.topic, self.nsqlookupd.topics()['topics'])

    def test_delete_channel(self):
        '''Can delete a channel within a topic'''
        self.nsqlookupd.delete_channel(self.topic, self.channel)
        self.assertNotIn(self.channel,
            self.nsqlookupd.channels(self.topic)['channels'])

    def test_create_topics(self):
        '''Can create a topic'''
        topic = uuid.uuid4().hex
        with self.delete_topic(topic):
            self.nsqlookupd.create_topic(topic)
            self.assertIn(topic, self.nsqlookupd.topics()['topics'])

    def test_create_channel(self):
        '''Can create a channel within a topic'''
        channel = uuid.uuid4().hex
        self.nsqlookupd.create_channel(self.topic, channel)
        self.assertIn(channel, self.nsqlookupd.channels(self.topic)['channels'])

    def test_debug(self):
        '''Can access debug information'''
        key = 'channel:%s:%s' % (self.topic.decode(), self.channel.decode())
        self.assertIn(key, self.nsqlookupd.debug())
