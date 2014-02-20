#! /usr/bin/env python

'''To run this benchmark, you'll need 10 instances of nsqd running locally. A
quick and easy way to get this going is:


for port in 415{0,2,3,4,5,6,7,8,9} 4162
do
  mkdir -p "nsqd/${port}"
  http_port=`echo $port | sed s#4#5#`
  nsqd --lookupd-tcp-address=127.0.0.1:4160 \
       --tcp-address=0.0.0.0:${port} \
       --http-address=0.0.0.0:${http_port} \
       --data-path=nsqd/${port} > /dev/null 2>&1 &
  # Optional -- abandon the job so it lives on daemonized
  disown
done
'''

from nsq.clients import nsqd
from nsq.reader import Reader

from contextlib import closing
from itertools import islice
import time


# The hosts described above
tcp_ports = [
  4150, 4152, 4153, 4154, 4155, 4156, 4157, 4158, 4159, 4162
]
tcp_hosts = ['localhost:%i' % port for port in tcp_ports]

http_ports = [
  4151, 5152, 5153, 5154, 5155, 5156, 5157, 5158, 5159, 5162
]
http_hosts = ['http://localhost:%i' % port for port in http_ports]

# The number of messages to send through for benchmarking
count = int(1e6)
# The topic and channel we'll listen to
topic = 'topic'
channel = 'channel'

# First, write out some messages
host_count = count / len(http_hosts)
for host in http_hosts:
    messages = map(str, xrange(host_count))
    print 'Sending %i messages to %s' % (host_count, host)
    nsqd.Client(host).mpub(topic, messages, binary=True)

# And now, we'll consume the messages
client = Reader(
  topic, channel, nsqd_tcp_addresses=tcp_hosts, max_in_flight=2500)
with closing(client):
    start = -time.time()
    for message in islice(client, count):
        # print message
        message.fin()
    start += time.time()
    print 'Finished %i messages in %fs (%5.2f messages / second)' % (
        count, start, count / start)
