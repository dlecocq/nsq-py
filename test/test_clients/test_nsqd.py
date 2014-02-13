from contextlib import contextmanager
import uuid

from nsq.clients import nsqd, ClientException
from nsq.util import pack
from common import ClientTest


class TestNsqdClient(ClientTest):
    '''Testing the nsqd client in isolation'''
    def setUp(self):
        self.client = nsqd.Client('http://foo:1')

    def test_mpub_ascii(self):
        '''Publishes ascii messages fine'''
        with self.patched_post() as post:
            post.return_value.content = 'OK'
            messages = map(str, range(10))
            self.client.mpub('topic', messages, binary=False)
            post.assert_called_with(
                '/mpub', params={'topic': 'topic'}, data='\n'.join(messages))

    def test_mpub_binary(self):
        '''Publishes messages with binary fine'''
        with self.patched_post() as post:
            post.return_value.content = 'OK'
            messages = map(str, range(10))
            self.client.mpub('topic', messages)
            post.assert_called_with(
                '/mpub', params={'topic': 'topic'}, data=pack(messages))

    def test_mpub_ascii_exception(self):
        '''Raises an exception when ascii-mpub-ing messages with newline'''
        messages = ['hello\n', 'how\n', 'are\n', 'you\n']
        self.assertRaises(
            ClientException, self.client.mpub, 'topic', messages, binary=False)


class TestNsqdClientIntegration(ClientTest):
    '''An integration test of the nsqd client'''
    def setUp(self):
        self.topic = 'foo-topic'
        self.channel = 'foo-channel'
        self.client = nsqd.Client('http://localhost:4151')
        try:
            self.client.ping()
        except ClientException:
            print 'Make sure nqsd is running locally on 4151'
            raise
        self.client.create_topic(self.topic)
        self.client.create_channel(self.topic, self.channel)

    def tearDown(self):
        try:
            self.client.delete_channel(self.topic, self.channel)
        except ClientException:
            pass
        self.client.delete_topic(self.topic)

    @contextmanager
    def delete_topic(self, topic):
        '''Delete a topic after running'''
        try:
            yield
        finally:
            self.client.delete_topic(topic)

    def test_ping_ok(self):
        '''Make sure ping works in a basic way'''
        self.assertEqual(self.client.ping(), 'OK')

    def test_info(self):
        '''Info works in a basic way'''
        self.assertIn('version', self.client.info()['data'])

    def test_pub(self):
        '''Publishing a message works as expected'''
        self.assertEqual(self.client.pub(self.topic, 'message'), 'OK')
        topic = self.client.clean_stats()['data']['topics'][self.topic]
        self.assertEqual(topic['channels'][self.channel]['depth'], 1)

    def test_mpub_ascii(self):
        '''Publishing messages in ascii works as expected'''
        messages = map(str, range(100))
        self.assertTrue(self.client.mpub(self.topic, messages, binary=False))

    def test_mpub_binary(self):
        '''Publishing messages in binary works as expected'''
        messages = map(str, range(100))
        self.assertTrue(self.client.mpub(self.topic, messages))

    def test_create_topic(self):
        '''Topic creation should work'''
        topic = uuid.uuid4().hex
        with self.delete_topic(topic):
            self.assertTrue(self.client.create_topic(topic))

    def test_empty_topic(self):
        '''We can drain a topic'''
        # This is pending, related to an issue:
        #   https://github.com/bitly/nsq/issues/313
        # self.client.pub(self.topic, 'foo')
        # topic = self.client.clean_stats()['data']['topics'][self.topic]
        # self.assertEqual(topic['channels'][self.channel]['depth'], 0)

    def test_delete_topic(self):
        '''We can delete a topic'''
        topic = uuid.uuid4().hex
        self.client.create_topic(topic)
        self.assertTrue(self.client.delete_topic(topic))
        self.assertRaises(ClientException, self.client.delete_topic, topic)

    def test_pause_topic(self):
        '''We can pause a topic'''
        self.assertTrue(self.client.pause_topic(self.topic))

    def test_unpause_topic(self):
        '''We can unpause a topic'''
        self.client.pause_topic(self.topic)
        self.assertTrue(self.client.unpause_topic(self.topic))

    def test_create_channel(self):
        '''We can create a channel'''
        # This is pending, related to an issue:
        #   https://github.com/bitly/nsq/issues/313
        # self.client.create_channel(self.topic, self.channel)
        # self.assertEqual(self.client.stats(), {})

    def test_clean_stats(self):
        '''Clean stats turns 'topics' and 'channels' into dictionaries'''
        stats = self.client.clean_stats()
        self.assertIsInstance(stats['data']['topics'], dict)
        self.assertIsInstance(
            stats['data']['topics'][self.topic]['channels'], dict)
