from zope.interface import implements

from txstatsd.metrics.metric import Metric
from txstatsd.itxstatsd import IMetric


class CGMetric(Metric): # TODO: what that this is for?
	def mark(self, item):
		'Report this item was seen.'
		self.send('{}|d'.format(item))


class CGMetricReporter(object):
	implements(IMetric)

	def __init__(self, name, wall_time_func=time.time, prefix=""):
		self.name = name
		self.wall_time_func = wall_time_func
		if prefix: prefix += "."
		self.prefix = prefix

	def process(self, fields):
		raise NotImplementedError
		# TODO: when do this one gets called?
		# self.update(fields[0])

	def update(self, item):
		# TODO: part of the interface?
		raise NotImplementedError
		# self.counter.add(self.wall_time_func(), item)

	def flush(self, interval, timestamp):
		# TODO: wall_time_func vs timestamp?
		# now = self.wall_time_func()
		import random
		return [(self.prefix + self.name + '.someval', random.randint(0, 100), timestamp)]
