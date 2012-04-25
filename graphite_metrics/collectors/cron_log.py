# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
import re, iso8601, calendar

from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


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


class CronJobs(Collector):

	lines, aliases = dict(), list()

	def __init__(self, *argz, **kwz):
		super(CronJobs, self).__init__(*argz, **kwz)

		try:
			src, self.lines, self.aliases =\
				op.attrgetter('source', 'lines', 'aliases')(self.conf)
			if not (src and self.lines and self.aliases): raise KeyError()
		except KeyError as err:
			if err.args:
				log.error('Failed to get required config parameter "{}"'.format(err.args[0]))
			else:
				log.warn( 'Collector requires all of "source",'
					' "lines" and "aliases" specified to work properly' )
			self.conf.enabled = False
			return

		for k,v in self.lines.viewitems(): self.lines[k] = re.compile(v)
		for idx,(k,v) in enumerate(self.aliases): self.aliases[idx] = k, re.compile(v)
		self.log_tailer = file_follow_durable( src,
			xattr_name=self.conf.xattr_name, read_interval_min=None )

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


collector = CronJobs
