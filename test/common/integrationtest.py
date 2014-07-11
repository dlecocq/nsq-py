import unittest

from contextlib import contextmanager

from nsq.http import ClientException, nsqd, nsqlookupd


class IntegrationTest(unittest.TestCase):
    '''For integration tests, includes a topic, a channel and clients'''
    def setUp(self):
        self.topic = 'foo-topic'
        self.channel = 'foo-channel'
        self.nsqd = nsqd.Client('http://localhost:4151')
        self.nsqlookupd = nsqlookupd.Client('http://localhost:4161')

        # Make sure we're connected to an instance
        try:
            self.nsqd.ping()
        except ClientException:
            print 'Make sure nqsd is running locally on 4150/4151'
            raise

        # Create this topic
        self.nsqd.create_topic(self.topic)
        self.nsqd.create_channel(self.topic, self.channel)

    def tearDown(self):
        with self.delete_topic(self.topic):
            pass

    @contextmanager
    def delete_topic(self, topic):
        '''Delete a topic after running'''
        try:
            yield
        finally:
            # Delete the topic from our nsqd instance
            try:
                self.nsqd.delete_topic(topic)
            except ClientException:
                pass
            # Delete the topic from our nsqlookupd instance
            try:
                self.nsqlookupd.delete_topic(topic)
            except ClientException:
                pass
