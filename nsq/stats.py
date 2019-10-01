from collections import Counter

from .http import nsqlookupd, nsqd

class Nsqlookupd(object):
    '''A class for grabbing stats about all hosts and topics reported to nsqlookupd.'''
    def __init__(self, *args, **kwargs):
        self.client = nsqlookupd.Client(*args, **kwargs)

    @property
    def merged(self):
        '''The clean stats from all the hosts reporting to this host.'''
        stats = {}
        for topic in self.client.topics()['topics']:
            for producer in self.client.lookup(topic)['producers']:
                hostname = producer['broadcast_address']
                port = producer['http_port']
                host = '%s_%s' % (hostname, port)
                stats[host] = nsqd.Client(
                    'http://%s:%s/' % (hostname, port)).clean_stats()
        return stats

    @property
    def raw(self):
        '''All the raw, unaggregated stats (with duplicates).'''
        topic_keys = (
            'message_count',
            'depth',
            'backend_depth',
            'paused'
        )

        channel_keys = (
            'in_flight_count',
            'timeout_count',
            'paused',
            'deferred_count',
            'message_count',
            'depth',
            'backend_depth',
            'requeue_count'
        )

        for host, stats in self.merged.items():
            for topic, stats in stats.get('topics', {}).items():
                for key in topic_keys:
                    value = int(stats.get(key, -1))
                    yield (
                        'host.%s.topic.%s.%s' % (host, topic, key),
                        value,
                        False
                    )
                    yield (
                        'topic.%s.%s' % (topic, key),
                        value,
                        True
                    )
                    yield (
                        'topics.%s' % key,
                        value,
                        True
                    )

                for chan, stats in stats.get('channels', {}).items():
                    data = {
                        key: int(stats.get(key, -1)) for key in channel_keys
                    }
                    data['clients'] = len(stats.get('clients', []))

                    for key, value in data.items():
                        yield (
                            'host.%s.topic.%s.channel.%s.%s' % (host, topic, chan, key),
                            value,
                            False
                        )
                        yield (
                            'host.%s.topic.%s.channels.%s' % (host, topic, key),
                            value,
                            True
                        )
                        yield (
                            'topic.%s.channels.%s' % (topic, key),
                            value,
                            True
                        )
                        yield (
                            'channels.%s' % key,
                            value,
                            True
                        )

    @property
    def stats(self):
        '''Stats that have been aggregated appropriately.'''
        data = Counter()
        for name, value, aggregated in self.raw:
            if aggregated:
                data['%s.max' % name] = max(data['%s.max' % name], value)
                data['%s.total' % name] += value
            else:
                data[name] = value

        return sorted(data.items())
