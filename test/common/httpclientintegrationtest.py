from contextlib import contextmanager

from nsq.http import ClientException, nsqd, nsqlookupd
from .integrationtest import IntegrationTest


class HttpClientIntegrationTest(IntegrationTest):
    '''For integration tests, includes a topic, a channel and clients'''
    nsqd_ports = (14150,)
    nsqlookup_port = 14160

    def setUp(self):
        self.topic = b'test-topic'
        self.channel = b'test-channel'
        self.nsqd = nsqd.Client('http://localhost:14151')
        self.nsqlookupd = nsqlookupd.Client('http://localhost:14161')

        # Create this topic
        self.nsqlookupd.create_topic(self.topic)
        self.nsqlookupd.create_channel(self.topic, self.channel)
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
