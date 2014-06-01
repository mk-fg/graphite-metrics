#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from lya import AttrDict, configure_logging
from collections import OrderedDict
import os, sys


def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect and dispatch various metrics to destinations.')
	parser.add_argument('-t', '--destination', metavar='host[:port]',
		help='host[:port] (default port: 2003, can be overidden'
			' via config file) of sink destination endpoint (e.g. carbon'
			' linereceiver tcp port, by default).')
	parser.add_argument('-i', '--interval', type=int, metavar='seconds',
		help='Interval between collecting and sending the datapoints.')

	parser.add_argument('-e', '--collector-enable',
		action='append', metavar='collector', default=list(),
		help='Enable only the specified metric collectors,'
				' can be specified multiple times.')
	parser.add_argument('-d', '--collector-disable',
		action='append', metavar='collector', default=list(),
		help='Explicitly disable specified metric collectors,'
			' can be specified multiple times. Overrides --collector-enable.')

	parser.add_argument('-s', '--sink-enable',
		action='append', metavar='sink', default=list(),
		help='Enable only the specified datapoint sinks,'
				' can be specified multiple times.')
	parser.add_argument('-x', '--sink-disable',
		action='append', metavar='sink', default=list(),
		help='Explicitly disable specified datapoint sinks,'
			' can be specified multiple times. Overrides --sink-enable.')

	parser.add_argument('-p', '--processor-enable',
		action='append', metavar='processor', default=list(),
		help='Enable only the specified datapoint processors,'
				' can be specified multiple times.')
	parser.add_argument('-z', '--processor-disable',
		action='append', metavar='processor', default=list(),
		help='Explicitly disable specified datapoint processors,'
			' can be specified multiple times. Overrides --processor-enable.')

	parser.add_argument('-c', '--config',
		action='append', metavar='path', default=list(),
		help='Configuration files to process.'
			' Can be specified more than once.'
			' Values from the latter ones override values in the former.'
			' Available CLI options override the values in any config.')

	parser.add_argument('-a', '--xattr-emulation', metavar='db-path',
		help='Emulate filesystem extended attributes (used in'
			' some collectors like sysstat or cron_log), storing per-path'
			' data in a simple shelve db.')
	parser.add_argument('-n', '--dry-run',
		action='store_true', help='Do not actually send data.')
	parser.add_argument('--debug-memleaks', action='store_true',
		help='Import guppy and enable its manhole to debug memleaks (requires guppy module).')
	parser.add_argument('--debug',
		action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	# Read configuration files
	cfg = AttrDict.from_yaml('{}.yaml'.format(
		os.path.splitext(os.path.realpath(__file__))[0] ))
	for k in optz.config: cfg.update_yaml(k)

	# Logging
	import logging
	configure_logging( cfg.logging,
		logging.DEBUG if optz.debug else logging.WARNING )
	if not cfg.logging.tracebacks:
		class NoTBLogger(logging.Logger):
			def exception(self, *argz, **kwz): self.error(*argz, **kwz)
		logging.setLoggerClass(NoTBLogger)
	log = logging.getLogger(__name__)

	# Manholes
	if optz.debug_memleaks:
		import guppy
		from guppy.heapy import Remote
		Remote.on()

	# Fill "auto-detected" blanks in the configuration, CLI overrides
	try:
		if optz.destination: cfg.sinks._default.host = optz.destination
		cfg.sinks._default.host = cfg.sinks._default.host.rsplit(':', 1)
		if len(cfg.sinks._default.host) == 1:
			cfg.sinks._default.host =\
				cfg.sinks._default.host[0], cfg.sinks._default.default_port
		else: cfg.sinks._default.host[1] = int(cfg.sinks._default.host[1])
	except KeyError: pass
	if optz.interval: cfg.loop.interval = optz.interval
	if optz.dry_run: cfg.debug.dry_run = optz.dry_run
	if optz.xattr_emulation: cfg.core.xattr_emulation = optz.xattr_emulation

	# Fake "xattr" module, if requested
	if cfg.core.xattr_emulation:
		import shelve
		xattr_db = shelve.open(cfg.core.xattr_emulation, 'c')
		class xattr_path(object):
			def __init__(self, base):
				assert isinstance(base, str)
				self.base = base
			def key(self, k): return '{}\0{}'.format(self.base, k)
			def __setitem__(self, k, v): xattr_db[self.key(k)] = v
			def __getitem__(self, k): return xattr_db[self.key(k)]
			def __del__(self): xattr_db.sync()
		class xattr_module(object): xattr = xattr_path
		sys.modules['xattr'] = xattr_module

	# Override "enabled" collector/sink parameters, based on CLI
	ep_conf = dict()
	for ep, enabled, disabled in\
			[ ('collectors', optz.collector_enable, optz.collector_disable),
				('processors', optz.processor_enable, optz.processor_disable),
				('sinks', optz.sink_enable, optz.sink_disable) ]:
		conf = cfg[ep]
		conf_base = conf.pop('_default')
		if 'debug' not in conf_base: conf_base['debug'] = cfg.debug
		ep_conf[ep] = conf_base, conf, OrderedDict(), enabled, disabled

	# Init global cfg for collectors/sinks' usage
	from graphite_metrics import collectors, sinks, loops
	collectors.cfg = sinks.cfg = loops.cfg = cfg

	# Init pluggable components
	import pkg_resources

	for ep_type in 'collector', 'processor', 'sink':
		ep_key = '{}s'.format(ep_type) # a bit of a hack
		conf_base, conf, objects, enabled, disabled = ep_conf[ep_key]
		ep_dict = dict( (ep.name, ep) for ep in
			pkg_resources.iter_entry_points('graphite_metrics.{}'.format(ep_key)) )
		eps = OrderedDict(
			(name, (ep_dict.pop(name), subconf or AttrDict()))
			for name, subconf in conf.viewitems() if name in ep_dict )
		eps.update( (name, (module, conf_base))
			for name, module in ep_dict.viewitems() )
		for ep_name, (ep_module, subconf) in eps.viewitems():
			if ep_name[0] == '_':
				log.debug( 'Skipping {} enty point,'
					' prefixed by underscore: {}'.format(ep_type, ep_name) )
			subconf.rebase(conf_base) # fill in "_default" collector parameters
			if enabled:
				if ep_name in enabled: subconf['enabled'] = True
				else: subconf['enabled'] = False
			if disabled and ep_name in disabled: subconf['enabled'] = False
			if subconf.get('enabled', True):
				log.debug('Loading {}: {}'.format(ep_type, ep_name))
				try: obj = getattr(ep_module.load(), ep_type)(subconf)
				except Exception as err:
					log.exception('Failed to load/init {} ({}): {}'.format(ep_type, ep_name, err))
					subconf.enabled = False
					obj = None
				if subconf.get('enabled', True): objects[ep_name] = obj
				else:
					log.debug(( '{} {} (entry point: {})'
						' was disabled after init' ).format(ep_type.title(), obj, ep_name))
		if ep_type != 'processor' and not objects:
			log.fatal('No {}s were properly enabled/loaded, bailing out'.format(ep_type))
			sys.exit(1)
		log.debug('{}: {}'.format(ep_key.title(), objects))

	loop = dict( (ep.name, ep) for ep in
		pkg_resources.iter_entry_points('graphite_metrics.loops') )
	conf = AttrDict(**cfg.loop)
	if 'debug' not in conf: conf.debug = cfg.debug
	loop = loop[cfg.loop.name].load().loop(conf)

	collectors, processors, sinks = it.imap( op.itemgetter(2),
		op.itemgetter('collectors', 'processors', 'sinks')(ep_conf) )
	log.debug(
		'Starting main loop: {} ({} collectors, {} processors, {} sinks)'\
		.format(loop, len(collectors), len(processors), len(sinks)) )
	loop.start(collectors, processors, sinks)

if __name__ == '__main__': main()
