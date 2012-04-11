# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from collections import deque
from contextlib import contextmanager
from io import open
import os, re, dbus, fcntl, stat

from . import Collector, Datapoint, user_hz, dev_resolve

import logging
log = logging.getLogger(__name__)


class CGAcct(Collector):


	def __init__(self, *argz, **kwz):
		super(CGAcct, self).__init__(*argz, **kwz)

		self.stuck_list = os.path.join(self.conf.cg_root, 'sticky.cgacct')

		# Check which info is available, if any
		self.rc_collectors = list()
		for rc in self.conf.resource_controllers:
			try: rc_collector = getattr(self, rc)
			except AttributeError:
				log.warn( 'Unable to find processor'
					' method for rc {!r} metrics, skipping it'.format(rc) )
				continue
			rc_path = os.path.join(self.conf.cg_root, rc)
			if not os.path.ismount(rc_path + '/'):
				log.warn(( 'Specified rc path ({}) does not'
					' seem to be a mountpoint, skipping it' ).format(rc_path))
				continue
			log.debug('Using cgacct collector for rc: {}'.format(rc))
			self.rc_collectors.append(rc_collector)

		if not self.rc_collectors: # no point doing anything else
			self.conf.enabled = False
			return

		# List of cgroup sticky bits, set by this service
		self._stuck_list_file = open(self.stuck_list, 'ab+')
		self._stuck_list = dict()
		fcntl.lockf(self._stuck_list_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
		self._stuck_list_file.seek(0)
		for line in self._stuck_list_file:
			rc, svc = line.strip().split()
			if rc not in self._stuck_list: self._stuck_list[rc] = set()
			self._stuck_list[rc].add(svc)


	def _cg_svc_dir(self, rc, svc=None):
		path = os.path.join(self.conf.cg_root, rc)
		if not svc: return path
		svc = svc.rsplit('@', 1)
		return os.path.join(path, 'system/{}.service'.format(svc[0] + '@'), svc[1])\
			if len(svc) > 1 else os.path.join(path, 'system/{}.service'.format(svc[0]))

	def _cg_svc_metrics(self, rc, metric, svc_instances):
		return (os.path.join( self._cg_svc_dir(rc, svc),
			'{}.{}'.format(rc, metric) ) for svc in svc_instances)

	@contextmanager
	def _cg_metric(self, path, **kwz):
		try:
			with open(path, mode='rb', **kwz) as src: yield src
		except (OSError, IOError) as err:
			log.debug('Failed to open cgroup metric: {}'.format(path, err))
			raise

	@staticmethod
	def _svc_name(svc): return svc.replace('@', '').replace('.', '_')


	@staticmethod
	def _systemd_services():
		for unit in dbus.Interface( dbus.SystemBus().get_object(
					'org.freedesktop.systemd1', '/org/freedesktop/systemd1' ),
				'org.freedesktop.systemd1.Manager' ).ListUnits():
			name, state = it.imap(str, op.itemgetter(0, 4)(unit))
			if name.endswith('.service') and state in ('running', 'start'): yield name[:-8]

	def _systemd_cg_stick(self, rc, services):
		if rc not in self._stuck_list: self._stuck_list[rc] = set()
		stuck_update, stuck = False, set(self._stuck_list[rc])
		services = set(services) # will be filtered and returned
		# Process services, make their cgroups persistent
		for svc in list(services):
			if svc not in stuck:
				svc_tasks = os.path.join(self._cg_svc_dir(rc, svc), 'tasks')
				try:
					os.chmod( svc_tasks,
						stat.S_IMODE(os.stat(svc_tasks).st_mode) | stat.S_ISVTX )
				except OSError: services.discard(svc) # not running
				else:
					self._stuck_list[rc].add(svc)
					stuck_update = True
			else: stuck.remove(svc) # to exclude it from the cleanup loop
		# Process stuck cgroups for removed services,
		#  try dropping these, otherwise just unstick and forget
		for svc in stuck:
			svc_dir = self._cg_svc_dir(rc, svc)
			try: os.rmdir(svc_dir)
			except OSError:
				log.debug( 'Non-empty cgroup for'
					' not-running service ({}): {}'.format(svc, svc_dir) )
				svc_tasks = os.path.join(svc_dir, 'tasks')
				try:
					os.chmod( svc_tasks,
						stat.S_IMODE(os.stat(svc_tasks).st_mode) & ~stat.S_ISVTX )
				except OSError:
					log.debug('Failed to unstick cgroup tasks file: {}'.format(svc_tasks))
			self._stuck_list[rc].remove(svc)
			stuck_update = True
		# Save list updates, if any
		if stuck_update:
			self._stuck_list_file.seek(0)
			self._stuck_list_file.truncate()
			for rc, stuck in self._stuck_list.viewitems():
				for svc in stuck: self._stuck_list_file.write('{} {}\n'.format(rc, svc))
			self._stuck_list_file.flush()
		return services

	_systemd_sticky_instances = lambda self, rc, services: (
		(self._svc_name(svc), list(svc_instances))
		for svc, svc_instances in it.groupby(
			sorted(set(services).intersection(self._systemd_cg_stick(rc, services))),
			key=lambda k: (k.rsplit('@', 1)[0]+'@' if '@' in k else k) ) )


	def cpuacct( self, services,
			_name = 'processes.services.{}.cpu.{}'.format,
			_stats=('user', 'system') ):
		## "stats" counters (user/system) are reported in USER_HZ - 1/Xth of second
		##  yielded values are in seconds, so counter should have 0-1 range,
		##  when divided by the interval
		## Not parsed: usage (should be sum of percpu)
		for svc, svc_instances in self._systemd_sticky_instances('cpuacct', services):
			if svc == 'total':
				log.warn('Detected service name conflict with "total" aggregation')
				continue
			# user/system jiffies
			stat = dict()
			for path in self._cg_svc_metrics('cpuacct', 'stat', svc_instances):
				try:
					with self._cg_metric(path) as src:
						for name, val in (line.strip().split() for line in src):
							if name not in _stats: continue
							try: stat[name] += int(val)
							except KeyError: stat[name] = int(val)
				except (OSError, IOError): pass
			for name in _stats:
				if name not in stat: continue
				yield Datapoint( _name(svc, name),
					'counter', float(stat[name]) / user_hz, None )
			# usage clicks
			usage = None
			for path in self._cg_svc_metrics('cpuacct', 'usage', svc_instances):
				try:
					with self._cg_metric(path) as src:
						usage = (0 if usage is None else usage) + int(src.read().strip())
				except (OSError, IOError): pass
			if usage is not None:
				yield Datapoint(_name(svc, 'usage'), 'counter', usage, None)


	@staticmethod
	def _iostat(pid, _conv=dict( read_bytes=('r', 1),
			write_bytes=('w', 1), cancelled_write_bytes=('w', -1),
			syscr=('rc', 1), syscw=('wc', 1) )):
		res = dict()
		for line in open('/proc/{}/io'.format(pid), 'rb'):
			line = line.strip()
			if not line: continue
			try: name, val = line.split(':', 1)
			except ValueError:
				log.warn('Unrecognized line format in proc/{}/io: {!r}'.format(pid, line))
				continue
			try: k,m = _conv[name]
			except KeyError: continue
			if k not in res: res[k] = 0
			res[k] += int(val.strip()) * m
		try: res = op.itemgetter('r', 'w', 'rc', 'wc')(res)
		except KeyError:
			raise OSError('Incomplete IO data for pid {}'.format(pid))
		# comm is used to make sure it's the same process
		return open('/proc/{}/comm'.format(pid), 'rb').read(), res

	@staticmethod
	def _read_ids(src):
		return set(it.imap(int, it.ifilter( None,
			it.imap(str.strip, src.readlines()) )))

	def blkio( self, services,
			_caches=deque([dict()], maxlen=2),
			_re_line = re.compile( r'^(?P<dev>\d+:\d+)\s+'
				r'(?P<iotype>Read|Write)\s+(?P<count>\d+)$' ),
			_name = 'processes.services.{}.io.{}'.format ):
		# Caches are for syscall io
		cache_prev = _caches[-1]
		cache_update = dict()

		for svc, svc_instances in self._systemd_sticky_instances('blkio', services):

			## Block IO
			## Only reads/writes are accounted, sync/async is meaningless now,
			##  because only sync ops are counted anyway
			svc_io = dict()
			for metric, src in [ ('bytes', 'io_service_bytes'),
					('time', 'io_service_time'), ('ops', 'io_serviced') ]:
				dst = svc_io.setdefault(metric, dict())
				for path in self._cg_svc_metrics('blkio', src, svc_instances):
					try:
						with self._cg_metric(path) as src:
							for line in src:
								match = _re_line.search(line.strip())
								if not match: continue # "Total" line, empty line
								dev = dev_resolve(*map(int, match.group('dev').split(':')))
								if dev is None: continue
								dev = dst.setdefault(dev, dict())
								iotype, val = match.group('iotype').lower(), int(match.group('count'))
								if iotype not in dev: dev[iotype] = val
								else: dev[iotype] += val
					except (OSError, IOError): pass
			for metric, devs in svc_io.viewitems():
				for dev, vals in devs.viewitems():
					if {'read', 'write'} != frozenset(vals):
						log.warn('Unexpected IO counter types: {}'.format(vals))
						continue
					for k,v in vals.viewitems():
						if not v: continue # no point writing always-zeroes for most devices
						yield Datapoint(_name( svc,
							'blkio.{}.{}_{}'.format(dev, metric, k) ), 'counter', v, None)

			## Syscall IO
			## Counters from blkio seem to be less useful in general,
			##  so /proc/*/io stats are collected for all processes in cgroup
			## Should be very inaccurate if pids are respawning
			tids, pids = set(), set()
			for base in it.imap(ft.partial(
					self._cg_svc_dir, 'blkio' ), svc_instances):
				try:
					with self._cg_metric(os.path.join(base, 'tasks')) as src:
						tids.update(self._read_ids(src)) # just to count them
					with self._cg_metric(os.path.join(base, 'cgroup.procs')) as src:
						pids.update(self._read_ids(src))
				except (OSError, IOError): continue
			# Process/thread count - only collected here
			yield Datapoint( 'processes.services.'
				'{}.threads'.format(svc), 'gauge', len(tids), None)
			yield Datapoint( 'processes.services.'
				'{}.processes'.format(svc), 'gauge', len(pids), None )

			# Actual io metrics
			svc_update = list()
			for pid in pids:
				try: comm, res = self._iostat(pid)
				except (OSError, IOError): continue
				svc_update.append(((svc, pid, comm), res))
			delta_total = list(it.repeat(0, 4))
			for k,res in svc_update:
				try: delta = map(op.sub, res, cache_prev[k])
				except KeyError: continue
				delta_total = map(op.add, delta, delta_total)
			for k,v in it.izip(['bytes_read', 'bytes_write', 'ops_read', 'ops_write'], delta_total):
				yield Datapoint(_name(svc, k), 'gauge', v, None)
			cache_update.update(svc_update)
		_caches.append(cache_update)


	def memory( self, services,
			_name = 'processes.services.{}.memory.{}'.format ):
		for svc, svc_instances in self._systemd_sticky_instances('memory', services):
			vals = dict()
			for path in self._cg_svc_metrics('memory', 'stat', svc_instances):
				try:
					with self._cg_metric(path) as src:
						for line in src:
							name, val = line.strip().split()
							if not name.startswith('total_'): continue
							name = name[6:]
							val, k = int(val), ( _name(svc, name),
								'gauge' if not name.startswith('pg') else 'counter' )
							if k not in vals: vals[k] = val
							else: vals[k] += val
				except (OSError, IOError): pass
			for (name, val_type), val in vals.viewitems():
				yield Datapoint(name, val_type, val, None)


	def read(self):
		services = list(self._systemd_services())
		for dp in it.chain.from_iterable(
			func(services) for func in self.rc_collectors ): yield dp


collector = CGAcct
