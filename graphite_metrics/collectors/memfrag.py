# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from io import open
import re

from . import Collector, Datapoint, page_size_kb

import logging
log = logging.getLogger(__name__)


class MemFrag(Collector):

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


collector = MemFrag
