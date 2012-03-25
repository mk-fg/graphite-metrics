from zope.interface import implements

from twisted.plugin import IPlugin
from txstatsd.itxstatsd import IMetricFactory
from systemd_cgacct.metrics import CGMetricReporter

class CGMetricFactory(object):
	implements(IMetricFactory, IPlugin)

	name = 'systemd_cgacct'
	metric_type = 'x' # TODO: no-match condition here

	def build_metric(self, prefix, name, wall_time_func=None):
		return CGMetricReporter(name, prefix=prefix, wall_time_func=wall_time_func)

	def configure(self, options):
		pass

systemd_cgacct_metric_factory = CGMetricFactory()
