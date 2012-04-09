# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
import re

from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


class MemStats(Collector):

	@staticmethod
	def _camelcase_fix( name,
			_re1=re.compile(r'(.)([A-Z][a-z]+)'),
			_re2=re.compile(r'([a-z0-9])([A-Z])'),
			_re3=re.compile(r'_+') ):
		return _re3.sub('_', _re2.sub(
			r'\1_\2', _re1.sub(r'\1_\2', name) )).lower()

	def read(self):
		# /proc/vmstat
		with open('/proc/vmstat', 'rb') as table:
			for line in table:
				metric, val = line.strip().split(None, 1)
				val = int(val)
				if metric.startswith('nr_'):
					yield Datapoint( 'memory.pages.allocation.{}'\
						.format(metric[3:]), 'gauge', val, None )
				else:
					yield Datapoint( 'memory.pages.activity.{}'\
						.format(metric), 'gauge', val, None )
		# /proc/meminfo
		with open('/proc/meminfo', 'rb') as table:
			table = dict(line.strip().split(None, 1) for line in table)
		hp_size = table.pop('Hugepagesize:', None)
		if hp_size and not hp_size.endswith(' kB'): hp_size = None
		if hp_size: hp_size = int(hp_size[:-3])
		else: log.warn('Unable to get hugepage size from /proc/meminfo')
		for metric, val in table.viewitems():
			if metric.startswith('DirectMap'): continue # static info
			# Name mangling
			metric = self._camelcase_fix(
				metric.rstrip(':').replace('(', '_').replace(')', '') )
			if metric.startswith('s_'): metric = 'slab_{}'.format(metric[2:])
			elif metric.startswith('mem_'): metric = metric[4:]
			elif metric == 'slab': metric = 'slab_total'
			# Value processing
			try: val, val_unit = val.split()
			except ValueError: # no units assumed as number of pages
				if not metric.startswith('huge_pages_'):
					log.warn( 'Unhandled page-measured'
						' metric in /etc/meminfo: {}'.format(metric) )
					continue
				val = int(val) * hp_size
			else:
				if val_unit != 'kB':
					log.warn('Unhandled unit type in /etc/meminfo: {}'.format(unit))
					continue
				val = int(val)
			yield Datapoint( 'memory.allocation.{}'\
				.format(metric), 'gauge', val * 1024, None )


collector = MemStats
