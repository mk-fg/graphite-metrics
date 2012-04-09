#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from subprocess import Popen, PIPE, STDOUT
from glob import iglob
from time import time, sleep, strptime, mktime
from calendar import timegm
from datetime import datetime, timedelta
from xattr import xattr
import os, sys, socket, struct

from graphite_metrics.utils import dev_resolve

try: from simplejson import loads, dumps
except ImportError: from json import loads, dumps

import logging
log = logging.getLogger()


class CarbonClient(object):

	def __init__(self, remote, max_reconnects=5, reconnect_delay=5):
		self.remote = remote
		if max_reconnects <= 0: max_reconnects = None
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

	def send(self, stat, val, ts):
		reconnects = self.max_reconnects
		msg = '{}\n'.format(' '.join([stat, bytes(val), bytes(ts)]))
		while True:
			try:
				self.sock.sendall(msg)
				return
			except socket.error as err:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				log.error('Failed to send data to Carbon server: {}'.format(err))
				self.reconnect()


def process_entry(entry, _sector_bytes=512):

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
				(prefix + ['bytes_read'], _sector_bytes * disk['rd_sec']),
				(prefix + ['bytes_write'], _sector_bytes * disk['wr_sec']),
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


def read_data( optz, ts_to=None, max_past_days=7,
		sa_path='/var/log/sa', xattr_name='user.sa_carbon.pos' ):
	if not ts_to: ts_to = datetime.now()

	sa_days = dict( (ts.day, ts) for ts in
		((ts_to - timedelta(i)) for i in xrange(max_past_days+1)) )
	sa_files = sorted(it.ifilter(
		op.methodcaller('startswith', 'sa'), os.listdir(sa_path) ))
	host = os.uname()[1]
	log.debug('SA files to process: {}'.format(sa_files))

	for sa in sa_files:
		sa_day = int(sa[2:])
		try: sa_day = sa_days[sa_day]
		except KeyError: continue # too old or new
		sa_ts_to = None # otherwise it's possible to get data for the oldest day in a file

		sa = os.path.join(sa_path, sa)
		log.debug('Processing file: {}'.format(sa))

		# Read xattr timestamp
		sa_xattr = xattr(sa)
		try: sa_ts_from = sa_xattr[xattr_name]
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
			if optz.strict:
				# Make sure that data inside matches filename
				sa_day_data = datetime.strptime(data['file-date'], '%Y-%m-%d')
				if sa_day - sa_day_data > timedelta(1):
					raise ValueError( 'Sa name/data timestamp'
						' mismatch: name={}, data={}, file={}'.format(sa_day, sa_day_data, sa) )
			sa_day_ts = mktime(sa_day.timetuple())
			# Read the data
			for ts, interval, metrics in it.ifilter(
					None, it.imap(process_entry, data['statistics']) ):
				if abs(ts - sa_day_ts) > 24*3600 + interval + 1:
					log.warn( 'Dropping sample because of timestamp mismatch'
						' (timestamp: {}, expected date: {})'.format(ts, sa_day_ts) )
					continue
				if optz.force_interval and interval != optz.force_interval:
					log.warn( 'Dropping sample because of interval mismatch'
						' (interval: {}, required: {}, timestamp: {})'.format(
							interval, optz.force_interval, ts ) )
					continue
				for name, val in metrics:
					name = [host] + list(name)
					yield name, val, ts
				ts -= 1 # has to be *before* beginning of the next interval
				if ts > sa_ts_max: sa_ts_max = ts

		# Update xattr timestamp, if any entries were processed
		if sa_ts_max:
			log.debug('Updating xattr timestamp to {}'.format(sa_ts_max))
			if not optz.dry_run: sa_xattr[xattr_name] = struct.pack('=I', int(sa_ts_max))


def dispatch_data(remote, data, dry_run=False, dump=False):
	log.debug('Establishing carbon server ({}) link'.format(remote))
	link = CarbonClient(remote)
	for name, val, ts in data:
		metric = '.'.join(name), val, int(ts)
		if dump: log.debug('Dispatching metric: {} {} {}'.format(*metric))
		if dry_run: link.send(*metric)
	log.debug('Severing carbon server link')
	link.close()


def main():
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('host', help='Carbon host to send data to.')
	parser.add_argument('-p', '--port', type=int, default=2003,
		help='Carbon tcp line-receiver port (default: %(default)s).')
	parser.add_argument('-i', '--force-interval', type=int,
		help='Discard datapoints for intervals (with a warning), different from this one.')
	parser.add_argument('-n', '--dry-run', action='store_true', help='Dry-run mode.')
	parser.add_argument('--strict', action='store_true', help='Bail out on some common sysstat bugs.')
	parser.add_argument('--debug', action='store_true', help='Dump a lot of debug info.')
	parser.add_argument('--debug-data', action='store_true', help='Dump processed datapoints.')
	optz = parser.parse_args()

	logging.basicConfig(
		level=logging.WARNING if not optz.debug else logging.DEBUG,
		format='%(levelname)s :: %(name)s :: %(message)s' )

	dispatch_data(
		(optz.host, optz.port), read_data(optz),
		dry_run=optz.dry_run, dump=optz.debug_data )

if __name__ == '__main__': main()
