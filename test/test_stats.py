'''Test our stats utility'''

import mock
import os
import unittest

import simplejson as json

from nsq.stats import Nsqlookupd
from nsq.http import nsqd
from common import IntegrationTest


class TestStats(IntegrationTest):
    '''Test our stats utility.'''

    nsqd_ports = (14150, 14152)

    def assertFixture(self, path, actual):
        '''Assert actual is equivalent to the JSON fixture provided.'''
        if os.environ.get('RECORD', 'false').lower() == 'true':
            with open(path, 'w') as fout:
                json.dump(actual,
                    fout, sort_keys=True, indent=4, separators=(',', ': ' ))
                # Add a trailing newline
                fout.write('\n')
        else:
            with open(path) as fin:
                self.assertEqual(json.load(fin), actual)

    def test_stats(self):
        '''Can effectively grab all the stats.'''
        # Create some topics, channels, and messages on the first nsqd instance
        client = nsqd.Client('http://localhost:14151')
        topics = [
            'topic-on-both-instances',
            'topic-with-channels',
            'topic-without-channels'
        ]
        for topic in topics:
            client.create_topic(topic)
            client.mpub(topic, map(str, range(len(topic))))
        client.create_channel('topic-with-channels', 'channel')

        # Create a topic and messages on the second nsqd isntance
        client = nsqd.Client('http://localhost:14153')
        client.create_topic('topic-on-both-instances')
        client.mpub(topic, map(str, range(10)))

        # Check the stats
        self.assertFixture(
            'test/fixtures/test_stats/TestStats/stats',
            map(list, Nsqlookupd('http://localhost:14161').stats))
