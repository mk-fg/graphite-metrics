# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from subprocess import Popen, PIPE
from collections import namedtuple, defaultdict
from io import open
import os, errno

from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


class IPTables(Collector):

	iptables = dict(ipv4='iptables-save', ipv6='ip6tables-save') # binaries
	metric_units = metric_tpl = None

	def __init__(self, *argz, **kwz):
		super(IPTables, self).__init__(*argz, **kwz)

		if not self.conf.rule_metrics_path.ipv4\
				and not self.conf.rule_metrics_path.ipv6:
			log.warn('No paths for rule_metrics_path specified')
			self.conf.enabled = False

		assert self.conf.units in ['pkt', 'bytes', 'both', 'both_flat']
		if self.conf.units.startswith('both'):
			self.metric_units = ['pkt', 'bytes']
			self.metric_tpl = '{}.{}' if self.conf.units == 'both' else '{}_{}'
		else: self.metric_units, self.metric_tpl = self.conf.units, '{}'


	_rule_metrics = namedtuple('RuleMetrics', 'table path mtime')
	_rule_metrics_cache = dict()

	@property
	def rule_metrics(self):
		rule_metrics = dict()
		for v in 'ipv4', 'ipv6':
			path = self.conf.rule_metrics_path[v]
			try:
				if not path: raise OSError()
				mtime = os.stat(path).st_mtime
			except (OSError, IOError) as err:
				if err.args and err.errno != errno.ENOENT: raise # to raise EPERM, EACCES and such
				self._rule_metrics_cache[v] = None
				continue
			cache = self._rule_metrics_cache.get(v)
			if not cache or path != cache.path or mtime != cache.mtime:
				log.debug('Detected rule_metrics file update: {} (cached: {})'.format(path, cache))
				metrics_table = dict()
				with open(path, 'rb') as src:
					for line in it.imap(op.methodcaller('strip'), src):
						if not line: continue
						table, chain, rule, metric = line.split(None, 3)
						metrics_table[table, chain, int(rule)] = metric
				cache = self._rule_metrics_cache[v]\
					= self._rule_metrics(metrics_table, path, mtime)
			rule_metrics[v] = cache
		return rule_metrics


	_table_hash = dict()

	def read(self):
		metric_counts = dict()
		hashes = defaultdict(lambda: defaultdict(list))

		for v, metrics in self.rule_metrics.viewitems():
			if not metrics: continue

			# Used to detect rule changes
			try:
				hash_old, metrics_old, warnings = self._table_hash[v]
				if metrics is not metrics_old: raise KeyError
			except KeyError: hash_old, warnings = None, dict()
			hash_new = hashes[v]

			# iptables-save invocation and output processing loop
			proc = Popen([self.iptables[v], '-c'], stdout=PIPE)
			chain_counts = defaultdict(int)
			for line in it.imap(op.methodcaller('strip'), proc.stdout):
				if line[0] != '[': # chain/table spec or comment
					if line[0] == '*': table = line[1:]
					continue
				counts, append, chain, rule = line.split(None, 3)
				assert append == '-A'

				rule_key = table, chain
				chain_counts[rule_key] += 1 # iptables rules are 1-indexed
				chain_count = chain_counts[rule_key]
				# log.debug('{}, Rule: {}'.format([table, chain, chain_count], rule))
				hash_new[rule_key].append(rule) # but py lists are 0-indexed
				try: metric = metrics.table[table, chain, chain_count]
				except KeyError: continue # no point checking rules w/o metrics attached
				# log.debug('Metric: {} ({}), rule: {}'.format(
				# 	metric, [table, chain, chain_count], rule ))

				# Check for changed rules
				try: rule_chk = hash_old and hash_old[rule_key][chain_count - 1]
				except (KeyError, IndexError): rule_chk = None
				if hash_old and rule_chk != rule:
					if chain_count not in warnings:
						log.warn(
							( 'Detected changed netfilter rule (chain: {}, pos: {})'
								' without corresponding rule_metrics file update: {}' )\
							.format(chain, chain_count, rule) )
						warnings[chain_count] = True
					if self.conf.discard_changed_rules: continue

				counts = map(int, counts.strip('[]').split(':', 1))
				try:
					metric_counts[metric] = list(it.starmap(
						op.add, it.izip(metric_counts[metric], counts) ))
				except KeyError: metric_counts[metric] = counts
			proc.wait()

			# Detect if there are any changes in the table,
			#  possibly messing the metrics, even if corresponding rules are the same
			hash_new = dict( (rule_key, tuple(rules))
				for rule_key, rules in hash_new.viewitems() )
			if hash_old\
					and frozenset(hash_old.viewitems()) != frozenset(hash_new.viewitems()):
				log.warn('Detected iptables changes without changes to rule_metrics file')
				hash_old = None
			if not hash_old: self._table_hash[v] = hash_new, metrics, dict()

		# Dispatch collected metrics
		for metric, counts in metric_counts.viewitems():
			for unit, count in it.izip(['pkt', 'bytes'], counts):
				if unit not in self.metric_units: continue
				yield Datapoint(self.metric_tpl.format(metric, unit), 'counter', count, None)


collector = IPTables
