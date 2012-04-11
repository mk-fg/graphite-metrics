# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from collections import namedtuple
from io import open

from . import Collector, Datapoint, page_size

import logging
log = logging.getLogger(__name__)


class SlabInfo(Collector):

	version_check = '2.1'

	def __init__(self, *argz, **kwz):
		super(SlabInfo, self).__init__(*argz, **kwz)

		for k in 'include_prefixes', 'exclude_prefixes':
			if not self.conf.get(k): self.conf[k] = list()

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
				for prefix in self.conf.include_prefixes:
					if info.name.startswith(prefix): break # force-include
				else:
					for prefix in self.conf.exclude_prefixes:
						if info.name.startswith(prefix):
							info = None
							break
				if info:
					vals = [
						('obj_active', info.active_objs * info.objsize),
						('slab_active', info.active_slabs * info.pagesperslab * ps),
						('slab_allocated', info.num_slabs * info.pagesperslab * ps) ]
					if self.conf.pass_zeroes or sum(it.imap(op.itemgetter(1), vals)) != 0:
						for val_name, val in vals:
							yield Datapoint( 'memory.slabs.{}.bytes_{}'\
								.format(info.name, val_name), 'gauge', val, None )


collector = SlabInfo
