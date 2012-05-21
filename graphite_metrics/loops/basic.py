# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft

from . import Loop

import logging
log = logging.getLogger(__name__)


class BasicLoop(Loop):

	'Simple synchronous "while True: fetch && process && send" loop.'

	def start(self, collectors, processors, sinks):
		from time import time, sleep

		ts = self.time_func()
		while True:
			data = list()
			for name, collector in collectors.viewitems():
				log.debug('Polling data from a collector (name: {}): {}'.format(name, collector))
				try: data.extend(collector.read())
				except Exception as err:
					log.exception( 'Failed to poll collector'
						' (name: {}, obj: {}): {}'.format(name, collector, err) )

			ts_now = self.time_func()
			sink_data = dict() # to batch datapoints on per-sink basis

			log.debug('Processing {} datapoints'.format(len(data)))
			for dp in it.ifilter(None, (dp.get(ts=ts_now) for dp in data)):
				proc_sinks = sinks.copy()
				for name, proc in processors.viewitems():
					if dp is None: break
					try: dp, sinks = proc.process(dp, sinks)
					except Exception as err:
						log.exception(( 'Failed to process datapoint (data: {},'
							' processor: {}, obj: {}): {}, discarding' ).format(dp, name, proc, err))
						break
				else:
					if dp is None: continue
					for name, sink in proc_sinks.viewitems():
						try: sink_data[name].append(dp)
						except KeyError: sink_data[name] = [dp]

			log.debug('Dispatching data to {} sink(s)'.format(len(sink_data)))
			if not self.conf.debug.dry_run:
				for name, tuples in sink_data.viewitems():
					log.debug(( 'Sending {} datapoints to sink'
						' (name: {}): {}' ).format(len(tuples), name, sink))
					try: sinks[name].dispatch(*tuples)
					except Exception as err:
						log.exception( 'Failed to dispatch data to sink'
							' (name: {}, obj: {}): {}'.format(name, sink, err) )

			while ts < ts_now: ts += self.conf.interval
			ts_sleep = max(0, ts - self.time_func())
			log.debug('Sleep: {}s'.format(ts_sleep))
			sleep(ts_sleep)


loop = BasicLoop
