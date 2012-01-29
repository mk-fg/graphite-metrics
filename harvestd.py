#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
log = logging.getLogger(__name__)


import itertools as it, operator as op, functools as ft
from collections import namedtuple, deque
from io import open
from time import time, sleep
from glob import iglob
from contextlib import contextmanager
import os, re, socket
import iso8601, calendar
import dbus, fcntl, stat


page_size = os.sysconf('SC_PAGE_SIZE')
page_size_kb = page_size // 1024
user_hz = os.sysconf('SC_CLK_TCK')


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



def file_follow( src, open_tail=True,
		read_interval_min=0.1,
			read_interval_max=20, read_interval_mul=1.1,
		rotation_check_interval=20, yield_file=False, **open_kwz ):
	from time import time, sleep
	from io import open
	import os, types

	open_tail = open_tail and isinstance(src, types.StringTypes)
	src_open = lambda: open(path, mode='rb', **open_kwz)
	stat = lambda f: (os.fstat(f) if isinstance(f, int) else os.stat(f))
	sanity_chk_stats = lambda stat: (stat.st_ino, stat.st_dev)
	sanity_chk_ts = lambda ts=None: (ts or time()) + rotation_check_interval

	if isinstance(src, types.StringTypes): src, path = None, src
	else:
		path = src.name
		src_inode, src_inode_ts =\
			sanity_chk_stats(stat(src.fileno())), sanity_chk_ts()
	line, read_chk = '', read_interval_min

	while True:

		if not src: # (re)open
			src = src_open()
			if open_tail:
				src.seek(0, os.SEEK_END)
				open_tail = False
			src_inode, src_inode_ts =\
				sanity_chk_stats(stat(src.fileno())), sanity_chk_ts()
			src_inode_chk = None

		ts = time()
		if ts > src_inode_ts: # rotation check
			src_inode_chk, src_inode_ts =\
				sanity_chk_stats(stat(path)), sanity_chk_ts(ts)
			if stat(src.fileno()).st_size < src.tell(): src.seek(0) # truncated
		else: src_inode_chk = None

		buff = src.readline()
		if not buff: # eof
			if src_inode_chk and src_inode_chk != src_inode: # rotated
				src.close()
				src, line = None, ''
				continue
			if read_chk is None:
				yield (buff if not yield_file else (buff, src))
			else:
				sleep(read_chk)
				read_chk *= read_interval_mul
				if read_chk > read_interval_max:
					read_chk = read_interval_max
		else:
			line += buff
			read_chk = read_interval_min

		if line and line[-1] == '\n': # complete line
			try:
				val = yield (line if not yield_file else (line, src))
				if val is not None: raise KeyboardInterrupt
			except KeyboardInterrupt: break
			line = ''

	src.close()


def file_follow_durable( path,
		min_dump_interval=10,
		xattr_name='user.collectd.logtail.pos',
		**follow_kwz ):
	'''Records log position into xattrs after reading line every
			min_dump_interval seconds.
		Checksum of the last line at the position
			is also recorded (so line itself don't have to fit into xattr) to make sure
			file wasn't truncated between last xattr dump and re-open.'''

	from xattr import xattr
	from io import open
	from hashlib import sha1
	from time import time
	import struct

	# Try to restore position
	src = open(path, mode='rb')
	src_xattr = xattr(src)
	try: pos = src_xattr[xattr_name]
	except KeyError: pos = None
	if pos:
		data_len = struct.calcsize('=I')
		(pos,), chksum = struct.unpack('=I', pos[:data_len]), pos[data_len:]
		(data_len,), chksum = struct.unpack('=I', chksum[:data_len]), chksum[data_len:]
		try:
			src.seek(pos - data_len)
			if sha1(src.read(data_len)).digest() != chksum:
				raise IOError('Last log line doesnt match checksum')
		except (OSError, IOError) as err:
			collectd.info('Failed to restore log position: {}'.format(err))
			src.seek(0)
	tailer = file_follow(src, yield_file=True, **follow_kwz)

	# ...and keep it updated
	pos_dump_ts_get = lambda ts=None: (ts or time()) + min_dump_interval
	pos_dump_ts = pos_dump_ts_get()
	while True:
		line, src_chk = next(tailer)
		if not line: pos_dump_ts = 0 # force-write xattr
		ts = time()
		if ts > pos_dump_ts:
			if src is not src_chk:
				src, src_xattr = src_chk, xattr(src_chk)
			pos_new = src.tell()
			if pos != pos_new:
				pos = pos_new
				src_xattr[xattr_name] =\
					struct.pack('=I', pos)\
					+ struct.pack('=I', len(line))\
					+ sha1(line).digest()
			pos_dump_ts = pos_dump_ts_get(ts)
		if (yield line.decode('utf-8', 'replace')):
			tailer.send(StopIteration)
			break


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
								yield Datapoint( 'memory.slabs.{}.bytes_{}'\
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
					yield Datapoint(name, 'counter', total, None)


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
							yield Datapoint( 'memory.fragmentation.{}'\
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
						yield Datapoint('irq.{}.{}'.format(irq, bind), 'counter', count, None)


	class CronJobs(Collector):

		log = '/var/log/processing/cron.log'

		lines = dict(
			init = r'task\[(\d+|-)\]: Queued\b[^:]*: (?P<job>.*)$',
			start = r'task\[(\d+|-)\]: Started\b[^:]*: (?P<job>.*)$',
			finish = r'task\[(\d+|-)\]: Finished\b[^:]*: (?P<job>.*)$',
			duration = r'task\[(\d+|-)\]:'\
				r' Finished \([^):]*\bduration=(?P<val>\d+)[,)][^:]*: (?P<job>.*)$',
			error = r'task\[(\d+|-)\]:'\
				r' Finished \([^):]*\bstatus=0*[^0]+0*[,)][^:]*: (?P<job>.*)$' )

		aliases = [
			('logrotate', r'(^|\b)logrotate\b'),
			('locate', r'(^|\b)updatedb\b'),
			('backup_grab', r'\bfs_backup\b'),
			('backup_toss', r'\btoss_cron\b'),
			('ufs_sync', r'\bufs\.sync\b'),
			('getmail', r'\bgetmail\.service\b'),
			('maildir_maintenance', r'\bmaildir_git\b'),
			('forager_music', r'\bforager_music\.py\b'),
			('forager_scm', r'\bforager_scm\.py\b'),
			('feedjack_update', r'\bfeedjack_update\.py\b'),
			('sync_distfiles', r'\bsync_distfiles\b'),
			('atop_rotate', r'\batop\.service\b'),
			('_name', r'\bsystemd: \S+ (?P<name>\S+)\b'),
			('_name', r'/etc/(\S+/)*(?P<name>\S+)(\s+|$)') ]

		def __init__(self):
			for k,v in self.lines.viewitems(): self.lines[k] = re.compile(v)
			for idx,(k,v) in enumerate(self.aliases): self.aliases[idx] = k, re.compile(v)
			self.log_tailer = file_follow_durable(
				self.log, read_interval_min=None )

		def read(self, _re_sanitize=re.compile('\s+|-')):
			# Cron
			if self.log_tailer:
				for line in iter(self.log_tailer.next, u''):
					# log.debug('LINE: {!r}'.format(line))
					ts, line = line.strip().split(None, 1)
					ts = calendar.timegm(iso8601.parse_date(ts).utctimetuple())
					for ev, regex in self.lines.viewitems():
						if not regex: continue
						match = regex.search(line)
						if match:
							job = match.group('job')
							for alias, regex in self.aliases:
								group = alias[1:] if alias.startswith('_') else None
								alias_match = regex.search(job)
								if alias_match:
									if group is not None:
										job = _re_sanitize.sub('_', alias_match.group(group))
									else: job = alias
									break
							else:
								log.warn('No alias for cron job: {!r}, skipping'.format(line))
								continue
							try: value = float(match.group('val'))
							except IndexError: value = 1
							# log.debug('TS: {}, EV: {}, JOB: {}'.format(ts, ev, job))
							yield Datapoint('cron.tasks.{}.{}'.format(job, ev), 'gauge', value, ts)


	class CGAcct(Collector):

		cg_root = '/sys/fs/cgroup'
		stuck_list = os.path.join(cg_root, 'sticky.cgacct')


		def __init__(self):
			# Check which info is available, if any
			self._collectors = list()
			for rc in os.listdir(self.cg_root):
				try: rc_collector = getattr(self, rc)
				except AttributeError: continue
				if not os.path.ismount(os.path.join(self.cg_root, rc) + '/'): continue
				log.debug('Adding cgroup collector: {}'.format(rc))
				self._collectors.append(rc_collector)
			if not self._collectors: return # no point doing anything else
			# List of cgroup sticky bits, set by this service
			self._stuck_list_file = open(self.stuck_list, 'ab+')
			self._stuck_list = dict()
			fcntl.lockf(self._stuck_list_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
			self._stuck_list_file.seek(0)
			for line in self._stuck_list_file:
				rc, svc = line.strip().split()
				if rc not in self._stuck_list: self._stuck_list[rc] = set()
				self._stuck_list[rc].add(svc)

		_cg_svc_dir = classmethod(
			lambda cls, rc, svc=None:\
				os.path.join( cls.cg_root, rc,
					'system/{}.service'.format(svc) if svc else '' ) )
		_cg_svc_metric = classmethod(
			lambda cls, rc, metric, svc=None, svc_dir=None:\
				os.path.join(
					svc_dir or cls._cg_svc_dir(rc, svc),
					'{}.{}'.format(rc, metric) ) )


		@staticmethod
		def _systemd_services():
			for unit in dbus.Interface( dbus.SystemBus().get_object(
						'org.freedesktop.systemd1', '/org/freedesktop/systemd1' ),
					'org.freedesktop.systemd1.Manager' ).ListUnits():
				name, state = it.imap(str, op.itemgetter(0, 4)(unit))
				if name.endswith('.service') and state in ('running', 'start'):
					name = name[:-8]
					if '@' in name: name = name.rsplit('@', 1)[0] + '@'
					yield name

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
				else: stuck.remove(svc)
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


		@classmethod
		@contextmanager
		def _cg_metric(cls, path, **kwz):
			try:
				with open(path, mode='rb', **kwz) as src: yield src
			except (OSError, IOError) as err:
				log.debug('Failed to open cgroup metric: {}'.format(path, err))
				raise

		@staticmethod
		def _svc_name(svc): return svc.replace('@', '').replace('.', '_')


		def cpuacct( self, services,
				_name = 'processes.services.{}.cpu.{}'.format ):
			## "stats" counters (user/system) are reported in USER_HZ - 1/Xth of second
			##  yielded values are in seconds, so counter should have 0-1 range,
			##  when divided by the interval
			## Not parsed: usage (should be sum of percpu)
			for svc in set(services).intersection(
					self._systemd_cg_stick('cpuacct', services) ):
				if svc == 'total':
					log.warn('Detected service name conflict with "total" aggregation')
					continue
				# user/system jiffies
				try:
					with self._cg_metric(self._cg_svc_metric('cpuacct', 'stat', svc)) as src:
						stat = dict(
							(name, int(val)) for name, val in
								(line.strip().split() for line in src) if name in ('user', 'system') )
				except (OSError, IOError): pass
				else:
					for name in 'user', 'system':
						yield Datapoint( _name(
							self._svc_name(svc), name),
							'counter', float(stat[name]) / user_hz, None )
				# usage clicks
				try:
					with self._cg_metric(self._cg_svc_metric('cpuacct', 'usage', svc)) as src:
						yield Datapoint(
							_name(self._svc_name(svc), 'usage'),
							'counter', int(src.read().strip()), None )
				except (OSError, IOError): pass


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
					r'(?P<iotype>Read|Write)\s+(?P<count>)\s*$' ),
				_name = 'processes.services.{}.io.{}'.format ):
			# Caches are for syscall io
			cache_prev = _caches[-1]
			cache_update = dict()

			for svc in set(services).intersection(
					self._systemd_cg_stick('blkio', services) ):

				## Block IO
				## Only reads/writes are accounted, sync/async is meaningless now,
				##  because only sync ops are counted anyway
				svc_io = dict()
				for metric, src in [ ('bytes', 'io_service_bytes'),
						('time', 'io_service_time'), ('ops', 'io_serviced') ]:
					try:
						with self._cg_metric(
								self._cg_svc_metric('blkio', src, svc) ) as src:
							dst = svc_io.setdefault(metric, dict())
							for line in src:
								match = _re_line.search(line)
								if not match: continue # "Total" line, empty line
								dev = dev_resolve(*map(int, match.group('dev').split(':')))
								if dev is None: continue
								dst = dst.setdefault(dev, dict())
								iotype, val = match.group('iotype').lower(), int(match.group('count'))
								if iotype in dev:
									log.warn( 'Duplicate entry for io type {!r},'
										' device {!r} in blkio accunting data'.format(iotype, dev) )
									dev[iotype] += val
								else: dev[iotype] = val
					except (OSError, IOError): pass
				for metric, devs in svc_io.viewitems():
					for dev, vals in devs.viewitems():
						if {'read', 'write'} != frozenset(vals):
							log.warn('Unexpected IO counter types: {}'.format(vals))
							continue
						for k,v in vals.viewitems():
							if not v: continue # no point writing always-zeroes for most devices
							yield Datapoint( _name(
								self._svc_name(svc), 'blkio.{}.{}_{}'.format(dev, metric, k) ),
								'counter', int(v), None )

				## Syscall IO
				## Counters from blkio seem to be less useful in general,
				##  so /proc/*/io stats are collected for all processes in cgroup
				## Should be very inaccurate if pids are respawning
				base = self._cg_svc_dir('blkio', svc)
				try:
					with self._cg_metric(os.path.join(base, 'tasks')) as src:
						tids = self._read_ids(src) # just to count them
					with self._cg_metric(os.path.join(base, 'cgroup.procs')) as src:
						pids = self._read_ids(src)
				except (OSError, IOError): continue
				# Process/thread count - only collected here
				yield Datapoint( 'processes.services.{}.threads'\
					.format(self._svc_name(svc)), 'gauge', len(tids), None )
				yield Datapoint( 'processes.services.{}.processes'\
					.format(self._svc_name(svc)), 'gauge', len(pids), None )
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
					yield Datapoint(_name(self._svc_name(svc), k), 'gauge', v, None)
				cache_update.update(svc_update)
			_caches.append(cache_update)


		def memory( self, services,
				_name = 'processes.services.{}.memory.{}'.format ):
			for svc in set(services).intersection(
					self._systemd_cg_stick('memory', services) ):
				try:
					with self._cg_metric(self._cg_svc_metric(
							'memory', 'stat', svc )) as src:
						for line in src:
							name, val = line.strip().split()
							if not name.startswith('total_'): continue
							name = name[6:]
							name, val, val_type = _name(self._svc_name(svc), name),\
								int(val), 'gauge' if not name.startswith('pg') else 'counter'
							yield Datapoint(name, val_type, val, None)
				except (OSError, IOError): pass


		def read(self):
			if not self._collectors: return # save dbus calls
			services = list(self._systemd_services())
			for dp in it.chain.from_iterable(func(services) for func in self._collectors): yield dp



def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect and dispatch various metrics to carbon daemon.')
	parser.add_argument('remote', help='host[:port] (default port: 2003) of carbon destination.')
	parser.add_argument('-i', '--interval', type=int, default=60,
		help='Interval between datapoints (default: %(default)s).')
	parser.add_argument('-n', '--dry-run', action='store_true', help='Do not actually send data.')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	logging.basicConfig(
		level=logging.WARNING if not optz.debug else logging.DEBUG,
		format='%(levelname)s :: %(name)s :: %(message)s' )

	if not optz.dry_run:
		link = optz.remote.rsplit(':', 1)
		if len(link) == 1: host, port = link[0], 2003
		else: host, port = link
		link = CarbonAggregator(os.uname()[1], (host, int(port)))

	collectors = list(
		collector() for collector in it.ifilter(
			lambda x: isinstance(x, type),
			vars(Collectors).values() ) )
	log.debug('Collectors: {}'.format(collectors))

	ts = time()
	while True:
		data = list(it.chain.from_iterable(
			it.imap(op.methodcaller('read'), collectors) ))
		ts_now = time()

		log.debug('Sending {} datapoints'.format(len(data)))
		if not optz.dry_run: link.send(ts_now, *data)

		while ts < ts_now: ts += optz.interval
		ts_sleep = max(0, ts - time())
		log.debug('Sleep: {}s'.format(ts_sleep))
		sleep(ts_sleep)

if __name__ == '__main__': main()
