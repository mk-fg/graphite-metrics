# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time

import requests

try: from simplejson import dumps
except ImportError: from json import dumps

from . import Sink

import logging
log = logging.getLogger(__name__)


class LibratoMetrics(Sink):

	'Interface to a Librato Metrics API v1. Uses JSON Array format.'

	def __init__(self, *argz, **kwz):
		super(LibratoMetrics, self).__init__(*argz, **kwz)

		# Try to set reasonable defaults
		if self.conf.http_parameters.timeout is None:
			try:
				from . import cfg
				self.conf.http_parameters.timeout = cfg.loop.interval / 2
			except (ImportError, KeyError): self.conf.http_parameters.timeout = 30
		if isinstance(self.conf.http_parameters.auth, list):
			self.conf.http_parameters.auth = tuple(self.conf.http_parameters.auth)

		requests.defaults.danger_mode = True
		requests.defaults.keep_alive = True
		requests.defaults.max_retries = max(3, self.conf.http_parameters.timeout / 5)

	def measurement(self, name, value, ts_dp=None):
		measurement = dict()
		if self.conf.source_from_prefix:
			measurement['source'], name = name.split('.', 1)
		elif self.conf.source: measurement['source'] = self.conf.source
		if ts_dp: measurement['measure_time'] = ts_dp
		measurement.update(name=name, value=value)
		return measurement

	def dispatch(self, *tuples):
		data = dict()
		if self.conf.unified_measure_time:
			data['measure_time'] = int(time())
			tuples = ((name, value, None) for name, value, ts_dp in tuples)
		data['gauges'] = list(it.starmap(self.measurement, tuples))
		requests.post( data=dumps(data),
				headers={'content-type': 'application/json'}, **self.conf.http_parameters )\
			.raise_for_status()


sink = LibratoMetrics
