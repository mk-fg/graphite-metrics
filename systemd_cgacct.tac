import socket
import random

from twisted.internet import reactor
from twisted.internet import task

from twisted.application.service import Application

from txstatsd.client import (
	TwistedStatsDClient, StatsDClientProtocol)
from txstatsd.metrics.metrics import Metrics

from txstatsd.report import ReportingService


STATSD_HOST = '127.0.0.1'
STATSD_PORT = 8125

application = Application('example-stats-client')
statsd_client = TwistedStatsDClient(STATSD_HOST, STATSD_PORT)
metrics = Metrics(connection=statsd_client,
	namespace=socket.gethostname() + '.example-client')

reporting = ReportingService()
reporting.setServiceParent(application)

def poller(prefix='cgacct'):
	return {prefix + '.a': random.randint(0, 10)}

reporting.schedule(poller, 10, metrics.increment)

protocol = StatsDClientProtocol(statsd_client)
reactor.listenUDP(0, protocol)
