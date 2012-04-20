# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time, sleep

from . import Loop

import logging
log = logging.getLogger(__name__)


class BasicLoop(Loop):

	'Simple synchronous "while True: fetch && send" loop.'

	def start(self, collectors, sinks):
		prefix = self.conf.prefix

		ts = self.time_func()
		while True:
			data = list()
			for collector in self.collectors:
				log.debug('Polling data from a collector: {}'.format(collector))
				try: data.extend(collector.read())
				except Exception as err:
					log.exception('Failed to poll collector ({}): {}'.format(collector, err))

			ts_now = self.time_func()
			data = filter(None, (dp.get(ts=ts_now, prefix=prefix) for dp in data))

			log.debug('Sending {} datapoints'.format(len(data)))
			if self.conf.debug.dump:
				for name, value, ts_dp in data:
					log.info('Datapoint: {} {} {}'.format(
						'{}.{}'.format(self.conf.hostname, name), value, ts_dp ))
			if not self.conf.debug.dry_run:
				for sink in self.sinks: sink.dispatch(*data)

			while ts < ts_now: ts += self.conf.interval
			ts_sleep = max(0, ts - self.time_func())
			log.debug('Sleep: {}s'.format(ts_sleep))
			sleep(ts_sleep)


loop = BasicLoop
