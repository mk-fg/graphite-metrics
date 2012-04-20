# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from subprocess import Popen, PIPE, STDOUT
from time import time, sleep, strptime, mktime
from calendar import timegm
from datetime import datetime, timedelta
from xattr import xattr
import os, sys, socket, struct

from . import Collector, Datapoint, dev_resolve, sector_bytes, rate_limit

try: from simplejson import loads, dumps
except ImportError: from json import loads, dumps

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
		except KeyError: return # happens, no idea what to do with these
		interval = ts['interval']
		ts = (mktime if not ts['utc'] else timegm)\
			(strptime('{} {}'.format(ts['date'], ts['time']), '%Y-%m-%d %H-%M-%S'))
		# Metrics
		metrics = list()

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

		if 'paging' in entry:
			stats = entry.pop('paging')
			metrics.append((['memory', 'pages', 'vm_efficiency'], stats['vmeff-percent']))

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
			for metric in stats['temperature']:
				name = ['sensors', 'temperature', metric['device'].replace('.', '_')]
				if 'number' in metric: name.append(bytes(metric['number']))
				metrics.append((name, metric['degC']))

		return ts, interval, metrics


	def _read(self, ts_to=None, max_past_days=7):
		if not ts_to: ts_to = datetime.now()

		sa_days = dict( (ts.day, ts) for ts in
			((ts_to - timedelta(i)) for i in xrange(max_past_days+1)) )
		sa_files = sorted(it.ifilter(
			op.methodcaller('startswith', 'sa'), os.listdir(self.conf.sa_path) ))
		host = os.uname()[1] # to check vs nodename in data
		log.debug('SA files to process: {}'.format(sa_files))

		for sa in sa_files:
			sa_day = int(sa[2:])
			try: sa_day = sa_days[sa_day]
			except KeyError: continue # too old or new
			sa_ts_to = None # otherwise it's possible to get data for the oldest day in a file

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
					# Last datapoint should go on the next day, but passing "-s 00:00:XX" will be wrong
					sa_ts_from = datetime(*sa_ts_from.timetuple()[:3]) + timedelta(1) - timedelta(seconds=1)

			# Get data from sadf
			proc = ['sadf', '-jt']
			if sa_ts_from: proc.extend(['-s', sa_ts_from.strftime('%H:%M:%S')])
			if sa_ts_to: proc.extend(['-e', sa_ts_to.strftime('%H:%M:%S')])
			proc.extend(['--', '-A'])
			proc.append(sa)
			log.debug('sadf command: {}'.format(proc))
			proc = Popen(proc, stdout=PIPE)
			data = loads(proc.stdout.read())
			if proc.wait(): raise RuntimeError('sadf exited with error status')

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
					if abs(ts - sa_day_ts) > 24*3600 + interval + 1:
						log.warn( 'Dropping sample because of timestamp mismatch'
							' (timestamp: {}, expected date: {})'.format(ts, sa_day_ts) )
						continue
					if self.force_interval and (
							interval < self.force_interval[0]
							or interval > self.force_interval[1] ):
						log.warn( 'Dropping sample because of interval mismatch'
							' (interval: {interval}, required: {margins[0]}-{margins[1]}, timestamp: {ts})'\
								.format(interval=interval, ts=ts, margins=self.force_interval) )
						continue
					ts_val = int(ts)
					for name, val in metrics:
						yield Datapoint('.'.join(name), 'gauge', val, ts_val)
					ts -= 1 # has to be *before* beginning of the next interval
					if ts > sa_ts_max: sa_ts_max = ts

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
