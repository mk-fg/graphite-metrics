# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from collections import namedtuple
from time import time
import os

import logging
log = logging.getLogger(__name__)


page_size = os.sysconf('SC_PAGE_SIZE')
page_size_kb = page_size // 1024
user_hz = os.sysconf('SC_CLK_TCK')


class Collector(object): pass


class Datapoint(namedtuple('Value', 'name type value ts')):
	_counter_cache = dict()
	_counter_cache_check_ts = 0
	_counter_cache_check_timeout = 12 * 3600
	_counter_cache_check_count = 4

	def _counter_cache_cleanup(self, ts, to):
		cleanup_list = list( k for k,(v,ts_chk) in
			self._counter_cache.viewitems() if (ts - to) > ts_chk )
		log.debug('Counter cache cleanup: {} buckets'.format(len(cleanup_list)))
		for k in cleanup_list: del self._counter_cache[k]

	def get(self, ts=None):
		ts = self.ts or ts or time()
		if ts > Datapoint._counter_cache_check_ts:
			self._counter_cache_cleanup( ts,
				self._counter_cache_check_timeout )
			Datapoint._counter_cache_check_ts = ts\
				+ self._counter_cache_check_timeout\
				/ self._counter_cache_check_count
		if self.type == 'counter':
			if self.name not in self._counter_cache:
				log.debug('Initializing bucket for new counter: {}'.format(self.name))
				self._counter_cache[self.name] = self.value, ts
				return None
			v0, ts0 = self._counter_cache[self.name]
			if ts == ts0:
				log.warn('Double-poll of a counter for {!r}'.format(self.name))
				return None
			value = float(self.value - v0) / (ts - ts0)
			self._counter_cache[self.name] = self.value, ts
			if value < 0:
				# TODO: handle overflows properly, w/ limits
				log.debug( 'Detected counter overflow'
					' (negative delta): {}, {} -> {}'.format(self.name, v0, self.value) )
				return None
		elif self.type == 'gauge': value = self.value
		else: raise TypeError('Unknown type: {}'.format(self.type))
		return self.name, value, ts
