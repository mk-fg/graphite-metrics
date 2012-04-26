# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time
import types

from requests.auth import HTTPBasicAuth
import requests

try: from simplejson import dumps
except ImportError: from json import dumps

from . import Sink

import logging
log = logging.getLogger(__name__)


class LibratoMetrics(Sink):

	'''Interface to a Librato Metrics API v1. Uses JSON Array format.
		Relevant part of the docs: http://dev.librato.com/v1/post/metrics'''

	def __init__(self, *argz, **kwz):
		super(LibratoMetrics, self).__init__(*argz, **kwz)

		# Try to set reasonable defaults
		if self.conf.http_parameters.timeout is None:
			try:
				from . import cfg
				self.conf.http_parameters.timeout = cfg.loop.interval / 2
			except (ImportError, KeyError): self.conf.http_parameters.timeout = 30
		self.conf.http_parameters.auth = HTTPBasicAuth(*self.conf.http_parameters.auth)

		requests.defaults.keep_alive = True
		requests.defaults.max_retries = max(3, self.conf.http_parameters.timeout / 5)

		# Try to init concurrent (async) dispatcher
		self.send = lambda chunk, **kwz: requests.post(data=chunk, **kwz)
		if self.conf.chunk_data.enabled or self.conf.chunk_data.enabled is None:
			try: from requests import async
			except RuntimeError as err:
				if self.conf.chunk_data.enabled: raise
				else:
					log.warn(( 'Failed to initialize requests.async'
						' engine (gevent module missing?): {}, concurrent'
						' (chunked) measurements submission will be disabled' ).format(err))
					self.conf.chunk_data.enabled = False
			else:
				self.conf.chunk_data.enabled = True
				if not self.conf.chunk_data.max_concurrent_requests\
						or self.conf.chunk_data.max_concurrent_requests <= 0:
					self.conf.chunk_data.max_concurrent_requests = None
				self.send = lambda *chunks, **kwz:\
					map( op.methodcaller('raise_for_status'),
						async.map(
							list(async.post(data=chunk, **kwz) for chunk in chunks),
							size=self.conf.chunk_data.max_concurrent_requests ) )

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
			tuples = list((name, value, None) for name, value, ts_dp in tuples)
		if self.conf.chunk_data.enabled\
				and len(tuples) > self.conf.chunk_data.max_chunk_size:
			chunks, n = list(), 0
			while n < len(tuples):
				n_to = n + self.conf.chunk_data.max_chunk_size
				chunk = data.copy()
				chunk['gauges'] = list(it.starmap(self.measurement, tuples[n:n_to]))
				chunks.append(chunk)
				n = n_to
			log.debug(( 'Splitting {} measurements'
				' into {} concurrent requests' ).format(len(tuples), len(chunks)))
			data = map(dumps, chunks)
			del tuples, chunk, chunks # to gc ram from this corpus of data
		else: # single chunk
			data['gauges'] = list(it.starmap(self.measurement, tuples))
			data = [dumps(data)]
			del tuples
		self.send(*data, headers={
			'content-type': 'application/json' }, **self.conf.http_parameters)


sink = LibratoMetrics
