# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from collections import namedtuple
from glob import iglob
from time import time
import os

import logging
log = logging.getLogger(__name__)


page_size = os.sysconf('SC_PAGE_SIZE')
page_size_kb = page_size // 1024
user_hz = os.sysconf('SC_CLK_TCK')
sector_bytes = 512

# Global configuration for harvestd,
#  intended to be set before initializing collectors,
#  but should not be really relied upon - can be empty.
cfg = dict()


def rate_limit(max_interval=20, sampling=3, f=lambda x: x):
	'''x rises by 1 from 0 on each iteraton, back to 0 on triggering.
		f(x) should rise up to f(max_interval) in some way (with default
			"f(x)=x" probability rises lineary with 100% chance on "x=max_interval").
		"sampling" affect probablility in an "c=1-(1-c0)*(1-c1)*...*(1-cx)" exponential way.'''
	from random import random
	val = 0
	val_max = float(f(max_interval))
	while True:
		if val % sampling == 0:
			trigger = random() > (val_max - f(val)) / val_max
			if trigger: val = 0
			yield trigger
		else: yield False
		val += 1


def dev_resolve( major, minor,
		log_fails=True, _cache = dict(), _cache_time=600 ):
	ts_now = time()
	while True:
		if not _cache: ts = 0
		else:
			dev = major, minor
			dev_cached, ts = (None, _cache[None])\
				if dev not in _cache else _cache[dev]
		# Update cache, if necessary
		if ts_now > ts + _cache_time:
			_cache.clear()
			for link in it.chain(iglob('/dev/mapper/*'), iglob('/dev/sd*')):
				link_name = os.path.basename(link)
				try: link_dev = os.stat(link).st_rdev
				except OSError: continue # EPERM, EINVAL
				_cache[(os.major(link_dev), os.minor(link_dev))] = link_name, ts_now
			_cache[None] = ts_now
			continue # ...and try again
		if dev_cached: dev_cached = dev_cached.replace('.', '_')
		elif log_fails:
			log.warn( 'Unable to resolve device'
				' from major/minor numbers: {}:{}'.format(major, minor) )
		return dev_cached


class Collector(object):

	def __init__(self, conf):
		self.conf = conf

	def read(self):
		raise NotImplementedError( 'Collector.read method should be'
			' overidden in collector subclasses to return list of Datapoint objects.' )
		# return [Datapoint(...), Datapoint(...), ...]


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

	def get(self, ts=None, prefix=None):
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
		name = self.name if not prefix else '{}.{}'.format(prefix, self.name)
		return name, value, int(ts)
