# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time
from glob import iglob
import os

import logging
log = logging.getLogger(__name__)


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
