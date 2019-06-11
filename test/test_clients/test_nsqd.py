import uuid

import six
from nsq.http import nsqd, ClientException
from nsq.util import pack
from common import HttpClientIntegrationTest, ClientTest


class TestNsqdClient(ClientTest):
    '''Testing the nsqd client in isolation'''
    def setUp(self):
        self.client = nsqd.Client('http://foo:1')

    def test_mpub_ascii(self):
        '''Publishes ascii messages fine'''
        with self.patched_post() as post:
            post.return_value.content = b'OK'
            messages = [six.text_type(n).encode() for n in range(10)]
            self.client.mpub('topic', messages, binary=False)
            post.assert_called_with(
                '/mpub', params={'topic': 'topic'}, data=b'\n'.join(messages))

    def test_mpub_binary(self):
        '''Publishes messages with binary fine'''
        with self.patched_post() as post:
            post.return_value.content = b'OK'
            messages = [six.text_type(n).encode() for n in range(10)]
            self.client.mpub('topic', messages)
            post.assert_called_with(
                'mpub',
                params={'topic': 'topic', 'binary': True},
                data=pack(messages)[4:])

    def test_mpub_ascii_exception(self):
        '''Raises an exception when ascii-mpub-ing messages with newline'''
        messages = [b'hello\n', b'how\n', b'are\n', b'you\n']
        self.assertRaises(
            ClientException, self.client.mpub, 'topic', messages, binary=False)


class TestNsqdClientIntegration(HttpClientIntegrationTest):
    '''An integration test of the nsqd client'''
    def test_ping_ok(self):
        '''Make sure ping works in a basic way'''
        self.assertEqual(self.nsqd.ping(), b'OK')

    def test_info(self):
        '''Info works in a basic way'''
        self.assertIn('version', self.nsqd.info())

    def test_pub(self):
        '''Publishing a message works as expected'''
        self.assertEqual(self.nsqd.pub(self.topic, 'message'), b'OK')
        topic = self.nsqd.clean_stats()['topics'][self.topic.decode()]
        self.assertEqual(topic['channels'][self.channel.decode()]['depth'], 1)

    def test_mpub_ascii(self):
        '''Publishing messages in ascii works as expected'''
        messages = [six.text_type(i).encode() for i in range(100)]
        self.assertTrue(self.nsqd.mpub(self.topic, messages, binary=False))

    def test_mpub_binary(self):
        '''Publishing messages in binary works as expected'''
        messages = [six.text_type(i).encode() for i in range(100)]
        self.assertTrue(self.nsqd.mpub(self.topic, messages))

    def test_create_topic(self):
        '''Topic creation should work'''
        topic = uuid.uuid4().hex
        with self.delete_topic(topic):
            # Ensure the topic doesn't exist beforehand
            self.assertNotIn(topic, self.nsqd.clean_stats()['topics'])
            self.assertTrue(self.nsqd.create_topic(topic))
            # And now it exists afterwards
            self.assertIn(topic, self.nsqd.clean_stats()['topics'])

    def test_empty_topic(self):
        '''We can drain a topic'''
        topic = uuid.uuid4().hex
        with self.delete_topic(topic):
            self.nsqd.pub(topic, 'foo')
            self.nsqd.empty_topic(topic)
            depth = self.nsqd.clean_stats()['topics'][topic]['depth']
            self.assertEqual(depth, 0)

    def test_delete_topic(self):
        '''We can delete a topic'''
        topic = uuid.uuid4().hex
        with self.delete_topic(topic):
            self.nsqd.create_topic(topic)
            self.assertTrue(self.nsqd.delete_topic(topic))
            # Ensure the topic doesn't exist afterwards
            self.assertNotIn(topic, self.nsqd.clean_stats()['topics'])

    def test_pause_topic(self):
        '''We can pause a topic'''
        self.assertTrue(self.nsqd.pause_topic(self.topic))

    def test_unpause_topic(self):
        '''We can unpause a topic'''
        self.nsqd.pause_topic(self.topic)
        self.assertTrue(self.nsqd.unpause_topic(self.topic))

    def test_create_channel(self):
        '''We can create a channel'''
        topic = uuid.uuid4().hex
        channel = uuid.uuid4().hex
        with self.delete_topic(topic):
            self.nsqd.create_topic(topic)
            self.nsqd.create_channel(topic, channel)
            topic = self.nsqd.clean_stats()['topics'][topic]
            self.assertIn(channel, topic['channels'])

    def test_empty_channel(self):
        '''Can clear the messages out in a channel'''
        self.nsqd.pub(self.topic, self.channel)
        self.nsqd.empty_channel(self.topic, self.channel)
        topic = self.nsqd.clean_stats()['topics'][self.topic.decode()]
        channel = topic['channels'][self.channel.decode()]
        self.assertEqual(channel['depth'], 0)

    def test_delete_channel(self):
        '''Can delete a channel in a topic'''
        self.nsqd.delete_channel(self.topic, self.channel)
        topic = self.nsqd.clean_stats()['topics'][self.topic.decode()]
        self.assertNotIn(self.channel, topic['channels'])

    def test_pause_channel(self):
        '''Can pause a channel'''
        self.nsqd.pause_channel(self.topic, self.channel)
        topic = self.nsqd.clean_stats()['topics'][self.topic.decode()]
        channel = topic['channels'][self.channel.decode()]
        self.assertTrue(channel['paused'])

    def test_unpause_channel(self):
        '''Can unpause a channel'''
        self.nsqd.pause_channel(self.topic, self.channel)
        self.nsqd.unpause_channel(self.topic, self.channel)
        topic = self.nsqd.clean_stats()['topics'][self.topic.decode()]
        channel = topic['channels'][self.channel.decode()]
        self.assertFalse(channel['paused'])

    def test_clean_stats(self):
        '''Clean stats turns 'topics' and 'channels' into dictionaries'''
        stats = self.nsqd.clean_stats()
        self.assertIsInstance(stats['topics'], dict)
        self.assertIsInstance(
            stats['topics'][self.topic.decode()]['channels'], dict)
