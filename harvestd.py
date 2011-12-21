#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
log = logging.getLogger(__name__)


import itertools as it, operator as op, functools as ft
from collections import namedtuple
from io import open
from time import time, sleep
import os, re, struct, socket

page_size = os.sysconf('SC_PAGE_SIZE')
page_size_kb = page_size // 1024


class CarbonAggregator(object):

	def __init__(self, host, remote, max_reconnects=None, reconnect_delay=5):
		self.host, self.remote = host.replace('.', '_'), remote
		if not max_reconnects or max_reconnects < 0: max_reconnects = None
		self.max_reconnects = max_reconnects
		self.reconnect_delay = reconnect_delay
		self.connect()

	def connect(self):
		reconnects = self.max_reconnects
		while True:
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				self.sock.connect(self.remote)
				log.debug('Connected to Carbon at {}:{}'.format(*self.remote))
				return
			except socket.error, e:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				log.info( 'Failed to connect to'
					' {0[0]}:{0[1]}: {1}'.format(self.remote, e) )
				if self.reconnect_delay: sleep(max(0, self.reconnect_delay))

	def reconnect(self):
		self.close()
		self.connect()

	def close(self):
		try: self.sock.close()
		except: pass


	def pack(self, datapoints, ts=None):
		return ''.join(it.imap(ft.partial(self.pack_datapoint, ts=ts), datapoints))

	def pack_datapoint(self, dp, ts=None):
		dp = dp.get(ts=ts)
		if dp is None: return ''
		name, value, ts = dp
		return bytes('{} {} {}\n'.format(
			'{}.{}'.format(self.host, name), value, int(ts) ))


	def send(self, ts, *datapoints):
		reconnects = self.max_reconnects
		packet = self.pack(datapoints, ts=ts)
		while True:
			try:
				self.sock.sendall(packet)
				return
			except socket.error as err:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				log.error('Failed to send data to Carbon server: {}'.format(err))
				self.reconnect()


class DataPoint(namedtuple('Value', 'name type value ts')):
	_counter_cache = dict()

	def get(self, ts=None):
		ts = self.ts or ts or time()
		if self.type == 'counter':
			if self.name not in self._counter_cache:
				log.debug('Initializing bucket for new counter: {}'.format(self.name))
				self._counter_cache[self.name] = self.value, ts
				return None
			v0, ts0 = self._counter_cache[self.name]
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


class Collector(object): pass

class Collectors(object):

	class SlabInfo(Collector):

		version_check = '2.1'
		include_prefixes = list()
		exclude_prefixes = ['kmalloc-', 'kmem_cache', 'dma-kmalloc-']
		pass_zeroes = False

		def __init__(self, **kwz):
			_no_value = object()
			for k,v in kwz:
				if getattr(self, k, _no_value) is _no_value:
					raise KeyError('Unrecognized option: {}'.format(k))
				setattr(self, k, v)
			with open('/proc/slabinfo', 'rb') as table:
				line = table.readline()
				self.version = line.split(':')[-1].strip()
				if self.version_check\
						and self.version != self.version_check:
					log.warn( 'Slabinfo header indicates'
							' different schema version (expecting: {}): {}'\
						.format(self.version_check, line) )
				line = table.readline().strip().split()
				if line[0] != '#' or line[1] != 'name':
					log.error('Unexpected slabinfo format, not processing it')
					return
				headers = dict(name=0)
				for idx,header in enumerate(line[2:], 1):
					if header[0] == '<' and header[-1] == '>': headers[header[1:-1]] = idx
				pick = 'name', 'active_objs', 'objsize', 'pagesperslab', 'active_slabs', 'num_slabs'
				picker = op.itemgetter(*op.itemgetter(*pick)(headers))
				record = namedtuple('slabinfo_record', ' '.join(pick))
				self.parse_line = lambda line: record(*( (int(val) if idx else val)
						for idx,val in enumerate(picker(line.strip().split())) ))

		# http://elinux.org/Slab_allocator
		def read(self):
			parse_line, ps = self.parse_line, page_size
			with open('/proc/slabinfo', 'rb') as table:
				table.readline(), table.readline() # header
				for line in table:
					info = parse_line(line)
					for prefix in self.include_prefixes:
						if info.name.startswith(prefix): break # force-include
					else:
						for prefix in self.exclude_prefixes:
							if info.name.startswith(prefix):
								info = None
								break
					if info:
						vals = [
							('obj_active', info.active_objs * info.objsize),
							('slab_active', info.active_slabs * info.pagesperslab * ps),
							('slab_allocated', info.num_slabs * info.pagesperslab * ps) ]
						if self.pass_zeroes or sum(it.imap(op.itemgetter(1), vals)) != 0:
							for val_name, val in vals:
								yield DataPoint( 'memory.slabs.{}.bytes_{}'\
									.format(info.name, val_name), 'gauge', val, None )


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
						yield DataPoint( 'memory.pages.allocation.{}'\
							.format(metric[3:]), 'gauge', val, None )
					else:
						yield DataPoint( 'memory.pages.activity.{}'\
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
				yield DataPoint( 'memory.allocation.{}'\
					.format(metric), 'gauge', val * 1024, None )


	class Stats(Collector):

		def read(self):
			with open('/proc/stat', 'rb') as table:
				for line in table:
					label, vals = line.split(None, 1)
					total = int(vals.split(None, 1)[0])
					if label == 'intr': name = 'irq.total.hard'
					elif label == 'softirq': name = 'irq.total.soft'
					elif label == 'processes': name = 'processes.forks'
					else: continue # no more useful data here
					yield DataPoint(name, 'counter', total, None)


	class Memfrag(Collector):

		def read( self,
				_re_buddyinfo=re.compile(r'^\s*Node\s+(?P<node>\d+)'
					r',\s+zone\s+(?P<zone>\S+)\s+(?P<counts>.*)$'),
				_re_ptinfo=re.compile(r'^\s*Node\s+(?P<node>\d+)'
					r',\s+zone\s+(?P<zone>\S+),\s+type\s+(?P<mtype>\S+)\s+(?P<counts>.*)$') ):
			mmap, pskb = dict(), page_size_kb

			# /proc/buddyinfo
			with open('/proc/buddyinfo', 'rb') as table:
				for line in it.imap(bytes.strip, table):
					match = _re_buddyinfo.search(line)
					if not match:
						log.warn('Unrecognized line in /proc/buddyinfo, skipping: {!r}'.format(line))
						continue
					node, zone = int(match.group('node')), match.group('zone').lower()
					counts = dict( ('{}k'.format(pskb*2**order),count)
						for order,count in enumerate(it.imap(int, match.group('counts').strip().split())) )
					if node not in mmap: mmap[node] = dict()
					if zone not in mmap[node]: mmap[node][zone] = dict()
					mmap[node][zone]['available'] = counts

			# /proc/pagetypeinfo
			with open('/proc/pagetypeinfo', 'rb') as table:
				page_counts_found = False
				while True:
					line = table.readline()
					if not line: break
					elif 'Free pages count' not in line:
						while line.strip(): line = table.readline()
						continue
					elif page_counts_found:
						log.warn( 'More than one free pages'
							' counters section found in /proc/pagetypeinfo' )
						continue
					else:
						page_counts_found = True
						for line in it.imap(bytes.strip, table):
							if not line: break
							match = _re_ptinfo.search(line)
							if not match:
								log.warn( 'Unrecognized line'
									' in /proc/pagetypeinfo, skipping: {!r}'.format(line) )
								continue
							node, zone, mtype = int(match.group('node')),\
								match.group('zone').lower(), match.group('mtype').lower()
							counts = dict( ('{}k'.format(pskb*2**order),count)
								for order,count in enumerate(it.imap(int, match.group('counts').strip().split())) )
							if node not in mmap: mmap[node] = dict()
							if zone not in mmap[node]: mmap[node][zone] = dict()
							mmap[node][zone][mtype] = counts
				if not page_counts_found:
					log.warn('Failed to find free pages counters in /proc/pagetypeinfo')

			# Dispatch values from mmap
			for node,zones in mmap.viewitems():
				for zone,mtypes in zones.viewitems():
					for mtype,counts in mtypes.viewitems():
						if sum(counts.viewvalues()) == 0: continue
						for size,count in counts.viewitems():
							yield DataPoint( 'memory.fragmentation.{}'\
									.format('.'.join(it.imap( bytes,
										['node_{}'.format(node),zone,mtype,size] ))),
								'gauge', count, None )


	class IRQ(Collector):

		@staticmethod
		def _parse_irq_table(table):
			irqs = dict()
			bindings = map(bytes.lower, table.readline().strip().split())
			bindings_cnt = len(bindings)
			for line in it.imap(bytes.strip, table):
				irq, line = line.split(None, 1)
				irq = irq.rstrip(':').lower()
				if irq in irqs:
					log.warn('Conflicting irq name/id: {!r}, skipping'.format(irq))
					continue
				irqs[irq] = map(int, line.split(None, bindings_cnt)[:bindings_cnt])
			return bindings, irqs

		def read(self):
			irq_tables = list()
			# /proc/interrupts
			with open('/proc/interrupts', 'rb') as table:
				irq_tables.append(self._parse_irq_table(table))
			# /proc/softirqs
			with open('/proc/softirqs', 'rb') as table:
				irq_tables.append(self._parse_irq_table(table))
			# dispatch
			for bindings, irqs in irq_tables:
				for irq, counts in irqs.viewitems():
					if sum(counts) == 0: continue
					for bind, count in it.izip(bindings, counts):
						yield DataPoint('irq.{}.{}'.format(irq, bind), 'counter', count, None)


def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect and dispatch various metrics to carbon daemon.')
	parser.add_argument('remote', help='host[:port] (default port: 2003) of carbon destination.')
	parser.add_argument('-i', '--interval', type=int, default=60,
		help='Interval between datapoints (default: %(default)s).')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	logging.basicConfig(
		level=logging.WARNING if not optz.debug else logging.DEBUG,
		format='%(levelname)s :: %(name)s :: %(message)s' )

	link = optz.remote.rsplit(':', 1)
	if len(link) == 1: host, port = link[0], 2003
	else: host, port = link
	link = CarbonAggregator(os.uname()[1], (host, int(port)))

	collectors = list(
		collector() for collector in filter(
			lambda x: isinstance(x, type),
			vars(Collectors).values() ) )
	log.debug('Collectors: {}'.format(collectors))

	ts = time()
	while True:
		data = list(it.chain.from_iterable(
			it.imap(op.methodcaller('read'), collectors) ))
		ts_now = time()
		log.debug('Sending {} datapoints'.format(len(data)))
		link.send(ts_now, *data)
		while ts < ts_now: ts += optz.interval
		ts_sleep = max(0, ts - time())
		log.debug('Sleep: {}s'.format(ts_sleep))
		sleep(ts_sleep)

if __name__ == '__main__': main()
