# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from subprocess import Popen, PIPE, STDOUT
from time import time, sleep, strptime, mktime
from calendar import timegm
from datetime import datetime, timedelta
from xattr import xattr
import os, sys, socket, struct

from . import Collector, Datapoint, dev_resolve, sector_bytes, rate_limit

try: from simplejson import loads, dumps, JSONDecodeError
except ImportError:
	from json import loads, dumps
	JSONDecodeError = ValueError

import logging
log = logging.getLogger(__name__)


class SADF(Collector):


	def __init__(self, *argz, **kwz):
		super(SADF, self).__init__(*argz, **kwz)

		# Set force_interval margins, if used
		if self.conf.force_interval:
			try:
				from . import cfg
				interval = cfg.loop.interval
			except (ImportError, KeyError, AttributeError):
				log.warn( 'Failed to apply force_interval option'
					' - unable to access global configuration to get data collection interval' )
				self.force_interval = None
			else:
				if self.conf.force_interval_fuzz:
					fuzz = interval * self.conf.force_interval_fuzz / 100.0
				else: fuzz = 0
				self.force_interval = interval - fuzz, interval + fuzz
		else: self.force_interval = None

		self.rate_limit = rate_limit(
				max_interval=self.conf.rate.max_interval,
				sampling=self.conf.rate.sampling )\
			if self.conf.rate.limiting_enabled else None


	def process_entry(self, entry):

		# Timestamp
		try: ts = entry.pop('timestamp')
		except KeyError:
			log.info( 'Detected sysstat entry'
				' without timestamp, skipping: {!r}'.format(entry) )
			return # happens, no idea what to do with these
		interval = ts['interval']
		for fmt in '%Y-%m-%d %H-%M-%S', '%Y-%m-%d %H:%M:%S':
			try:
				ts = (mktime if not ts['utc'] else timegm)\
					(strptime('{} {}'.format(ts['date'], ts['time']), fmt))
			except ValueError: pass
			else: break
		else:
			raise ValueError( 'Unable to process'
				' sysstat timestamp: {!r} {!r}'.format(ts['date'], ts['time']) )

		# Metrics
		metrics = list()

		if self.conf.skip.sections:
			for k in self.conf.skip.sections:
				if k in entry: del entry[k]
				else: log.debug('Section-to-skip {!r} not found in sysstat entry'.format(k))
		process_redundant = not self.conf.skip.redundant

		if 'cpu-load-all' in entry:
			for stats in entry.pop('cpu-load-all'):
				prefix = stats.pop('cpu')
				if prefix == 'all': continue # can be derived by aggregator/webapp
				prefix = ['cpu', prefix]
				metrics.extend((prefix + [k], v) for k,v in stats.viewitems())

		if 'process-and-context-switch' in entry:
			stats = entry.pop('process-and-context-switch')
			metrics.append((['misc', 'contextswitch'], stats['cswch']))
			if process_redundant: # also processed in "stats"
				metrics.append((['processes', 'forks'], stats['proc']))

		if process_redundant:
			if 'interrupts' in entry: # with "irq"
				for stats in entry.pop('interrupts'):
					if stats['intr'] == 'sum': continue # can be derived by aggregator/webapp
					metrics.append((['irq', stats['intr'], 'sum'], stats['value']))
			if 'swap-pages' in entry: # with "memstats"
				for k,v in entry.pop('swap-pages').viewitems():
					metrics.append((['memory', 'pages', 'activity', k], v))
			# if 'memory' in entry: # with "memstats"
			# if 'hugepages' in entry: # with "memstats"

		if 'disk' in entry:
			for disk in entry.pop('disk'):
				dev_sadf = disk['disk-device']
				if not dev_sadf.startswith('dev'):
					log.warn('Unknown device name format: {}, skipping'.format(dev_sadf))
					continue
				dev = dev_resolve(*it.imap(int, dev_sadf[3:].split('-')), log_fails=False)
				if dev is None:
					log.warn('Unable to resolve name for device {!r}, skipping'.format(dev_sadf))
					continue
				prefix = ['disk', 'load', dev]
				metrics.extend([
					(prefix + ['utilization'], disk['util-percent']),
					(prefix + ['req_size'], disk['avgrq-sz']),
					(prefix + ['queue_len'], disk['avgqu-sz']),
					(prefix + ['bytes_read'], sector_bytes * disk['rd_sec']),
					(prefix + ['bytes_write'], sector_bytes * disk['wr_sec']),
					(prefix + ['serve_time'], disk['await']),
					(prefix + ['tps'], disk['tps']) ])
		# if 'io' in entry: # can be derived by aggregator/webapp

		if 'paging' in entry:
			metrics.append((
				['memory', 'pages', 'vm_efficiency'],
				entry.pop('paging')['vmeff-percent'] ))
			# XXX: lots of redundant metrics here

		if 'queue' in entry:
			stats = entry.pop('queue')
			for n in 1, 5, 15:
				k = 'ldavg-{}'.format(n)
				metrics.append((['load', k], stats[k]))
			metrics.extend(
				(['processes', 'state', k], stats[k])
				for k in ['runq-sz', 'plist-sz', 'blocked'] )

		if 'kernel' in entry:
			stats = entry.pop('kernel')
			metrics.extend([
				(['misc', 'dent_unused'], stats['dentunusd']),
				(['misc', 'file_handles'], stats['file-nr']),
				(['misc', 'inode_handles'], stats['inode-nr']),
				(['misc', 'pty'], stats['pty-nr']) ])

		if 'network' in entry:
			stats = entry.pop('network')
			iface_stats = stats.get('net-dev', list())
			for iface in iface_stats:
				prefix = ['network', 'interfaces', iface['iface']]
				metrics.extend([
					(prefix + ['rx', 'bytes'], iface['rxkB'] * 2**10),
					(prefix + ['rx', 'packets', 'total'], iface['rxpck']),
					(prefix + ['rx', 'packets', 'compressed'], iface['rxcmp']),
					(prefix + ['rx', 'packets', 'multicast'], iface['rxmcst']),
					(prefix + ['tx', 'bytes'], iface['txkB'] * 2**10),
					(prefix + ['tx', 'packets', 'total'], iface['txpck']),
					(prefix + ['tx', 'packets', 'compressed'], iface['txpck']) ])
			iface_stats = stats.get('net-edev', list())
			iface_errs_common = [('err', 'total'), ('fifo', 'overflow_fifo'), ('drop', 'overflow_kbuff')]
			for iface in iface_stats:
				prefix = ['network', 'interfaces', iface['iface']]
				for src,dst in iface_errs_common + [('fram', 'frame_alignment')]:
					metrics.append((prefix + ['rx', 'errors', dst], iface['rx{}'.format(src)]))
				for src,dst in iface_errs_common + [('carr', 'carrier')]:
					metrics.append((prefix + ['tx', 'errors', dst], iface['tx{}'.format(src)]))
				metrics.append((prefix + ['tx', 'errors', 'collision'], iface['coll']))
			if 'net-nfs' in stats:
				for k,v in stats['net-nfs'].viewitems():
					metrics.append((['network', 'nfs', 'client', k], v))
				for k,v in stats['net-nfsd'].viewitems():
					metrics.append((['network', 'nfs', 'server', k], v))
			if 'net-sock' in stats:
				for k,v in stats['net-sock'].viewitems():
					if k.endswith('sck'):
						k = k[:-3]
						if k == 'tot': k = 'total'
						metrics.append((['network', 'sockets', k], v))

		if 'power-management' in entry:
			stats = entry.pop('power-management')
			for metric in stats.get('temperature', list()):
				name = ['sensors', 'temperature', metric['device'].replace('.', '_')]
				if 'number' in metric: name.append(bytes(metric['number']))
				metrics.append((name, metric['degC']))

		return ts, interval, metrics


	def _read(self, ts_to=None):
		if not ts_to: ts_to = datetime.now()

		sa_days = dict( (ts.day, ts)
			for ts in ((ts_to - timedelta(i))
			for i in xrange(self.conf.skip.older_than_days+1)) )
		sa_files = sorted(it.ifilter(
			op.methodcaller('startswith', 'sa'), os.listdir(self.conf.sa_path) ))
		host = os.uname()[1] # to check vs nodename in data
		log.debug('SA files to process: {}'.format(sa_files))

		for sa in sa_files:
			sa_day = int(sa[2:])
			try: sa_day = sa_days[sa_day]
			except KeyError: continue # too old or new

			sa = os.path.join(self.conf.sa_path, sa)
			log.debug('Processing file: {}'.format(sa))

			# Read xattr timestamp
			sa_xattr = xattr(sa)
			try: sa_ts_from = sa_xattr[self.conf.xattr_name]
			except KeyError: sa_ts_from = None
			if sa_ts_from:
				sa_ts_from = datetime.fromtimestamp(
					struct.unpack('=I', sa_ts_from)[0] )
				if sa_day - sa_ts_from > timedelta(1) + timedelta(seconds=60):
					log.debug( 'Discarding xattr timestamp, because'
						' it doesnt seem to belong to the same date as file'
						' (day: {}, xattr: {})'.format(sa_day, sa_ts_from) )
					sa_ts_from = None
				if sa_ts_from and sa_ts_from.date() != sa_day.date():
					log.debug('File xattr timestamp points to the next day, skipping file')
					continue
			if not self.conf.max_dump_span: sa_ts_to = None
			else:
				# Use 00:00 of sa_day + max_dump_span if there's no xattr
				ts = sa_ts_from or datetime(sa_day.year, sa_day.month, sa_day.day)
				sa_ts_to = ts + timedelta(0, self.conf.max_dump_span)
				# Avoid adding restrictions, if they make no sense anyway
				if sa_ts_to >= datetime.now(): sa_ts_to = None

			# Get data from sadf
			sa_cmd = ['sadf', '-jt']
			if sa_ts_from: sa_cmd.extend(['-s', sa_ts_from.strftime('%H:%M:%S')])
			if sa_ts_to: sa_cmd.extend(['-e', sa_ts_to.strftime('%H:%M:%S')])
			sa_cmd.extend(['--', '-A'])
			sa_cmd.append(sa)
			log.debug('sadf command: {}'.format(sa_cmd))
			sa_proc = Popen(sa_cmd, stdout=PIPE)
			try: data = loads(sa_proc.stdout.read())
			except JSONDecodeError as err:
				log.exception(( 'Failed to process sadf (file:'
					' {}, command: {}) output: {}' ).format(sa, sa_cmd, err))
				data = None
			if sa_proc.wait():
				log.error('sadf (command: {}) exited with error'.format(sa_cmd))
				data = None
			if not data:
				log.warn('Skipping processing of sa file: {}'.format(sa))
				continue

			# Process and dispatch the datapoints
			sa_ts_max = 0
			for data in data['sysstat']['hosts']:
				if data['nodename'] != host:
					log.warn( 'Mismatching hostname in sa data:'
						' {} (uname: {}), skipping'.format(data['nodename'], host) )
					continue
				sa_day_ts = mktime(sa_day.timetuple())
				# Read the data
				for ts, interval, metrics in it.ifilter(
						None, it.imap(self.process_entry, data['statistics']) ):
					if ts - 1 > sa_ts_max:
						# has to be *before* beginning of the next interval
						sa_ts_max = ts - 1
					if abs(ts - sa_day_ts) > 24*3600 + interval + 1:
						log.warn( 'Dropping sample because of timestamp mismatch'
							' (timestamp: {}, expected date: {})'.format(ts, sa_day_ts) )
						continue
					if self.force_interval and (
							interval < self.force_interval[0]
							or interval > self.force_interval[1] ):
						log.warn( 'Dropping sample because of interval mismatch'
							' (file: {sa}, interval: {interval},'
							' required: {margins[0]}-{margins[1]}, timestamp: {ts})'\
								.format(sa=sa, interval=interval, ts=ts, margins=self.force_interval) )
						continue
					ts_val = int(ts)
					for name, val in metrics:
						yield Datapoint('.'.join(name), 'gauge', val, ts_val)

			# Update xattr timestamp, if any entries were processed
			if sa_ts_max:
				log.debug('Updating xattr timestamp to {}'.format(sa_ts_max))
				if not self.conf.debug.dry_run:
					sa_xattr[self.conf.xattr_name] = struct.pack('=I', int(sa_ts_max))


	def read(self):
		if not self.rate_limit or next(self.rate_limit):
			log.debug('Running sysstat data processing cycle')
			return self._read()
		else: return list()


collector = SADF
