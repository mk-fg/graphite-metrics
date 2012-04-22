#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from collections import Mapping
import os, sys



class AttrDict(dict):

	def __init__(self, *argz, **kwz):
		for k,v in dict(*argz, **kwz).iteritems(): self[k] = v

	def __setitem__(self, k, v):
		super(AttrDict, self).__setitem__( k,
			AttrDict(v) if isinstance(v, Mapping) else v )
	def __getattr__(self, k):
		if not k.startswith('__'): return self[k]
		else: raise AttributeError # necessary for stuff like __deepcopy__ or __hash__
	def __setattr__(self, k, v): self[k] = v

	@classmethod
	def from_yaml(cls, path, if_exists=False):
		import yaml
		if if_exists and not os.path.exists(path): return cls()
		return cls(yaml.load(open(path)))

	def flatten(self, path=tuple()):
		dst = list()
		for k,v in self.iteritems():
			k = path + (k,)
			if isinstance(v, Mapping):
				for v in v.flatten(k): dst.append(v)
			else: dst.append((k, v))
		return dst

	def update_flat(self, val):
		if isinstance(val, AttrDict): val = val.flatten()
		for k,v in val:
			dst = self
			for slug in k[:-1]:
				if dst.get(slug) is None:
					dst[slug] = AttrDict()
				dst = dst[slug]
			if v is not None or not isinstance(
				dst.get(k[-1]), Mapping ): dst[k[-1]] = v

	def update_yaml(self, path):
		self.update_flat(self.from_yaml(path))


def configure_logging(cfg, custom_level=None):
	import logging, logging.config
	if custom_level is None: custom_level = logging.WARNING
	for entity in it.chain.from_iterable(it.imap(
			op.methodcaller('viewvalues'),
			[cfg] + list(cfg.get(k, dict()) for k in ['handlers', 'loggers']) )):
		if isinstance(entity, Mapping)\
			and entity.get('level') == 'custom': entity['level'] = custom_level
	logging.config.dictConfig(cfg)


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

	parser.add_argument('-c', '--config',
		action='append', metavar='path', default=list(),
		help='Configuration files to process.'
			' Can be specified more than once.'
			' Values from the latter ones override values in the former.'
			' Available CLI options override the values in any config.')

	parser.add_argument('-n', '--dry-run',
		action='store_true', help='Do not actually send data.')
	parser.add_argument('--debug-data', action='store_true',
		help='Log datapoints that are (unless --dry-run is used) sent to carbon.')
	parser.add_argument('--debug',
		action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	# Read configuration files
	cfg = AttrDict.from_yaml('{}.yaml'.format(
		os.path.splitext(os.path.realpath(__file__))[0] ))
	for k in optz.config: cfg.update_yaml(k)

	# Deprecated stuff
	if cfg.get('core') or cfg.get('carbon'):
		raise ValueError(
			'Old-style loop/sink configuration options usage detected.'
			' These are no longer supported or used.'
			' Please move these into "loop" section of the configuration file.' )

	# Logging
	import logging
	global log # will be initialized with default level otherwise
	configure_logging( cfg.logging,
		logging.DEBUG if optz.debug else logging.WARNING )
	logging.captureWarnings(cfg.logging.warnings)
	log = logging.getLogger(__name__)
	if not cfg.logging.tracebacks: log.exception = log.error

	# Fill "auto-detected" blanks in the configuration, CLI overrides
	if cfg.loop.prefix is None: cfg.loop.prefix = os.uname()[1].replace('.', '_')
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
	if optz.debug_data: cfg.debug.dump = optz.debug_data

	# Override "enabled" collector/sink parameters, based on CLI
	ep_conf = dict()
	for ep, enabled, disabled in\
			[ ('collectors', optz.collector_enable, optz.collector_disable),
				('sinks', optz.sink_enable, optz.sink_disable) ]:
		conf = cfg[ep]
		conf_base = conf.pop('_default')
		if enabled:
			for name, subconf in conf.viewitems():
				if name not in enabled: subconf['enabled'] = False
			for name in enabled:
				if name not in conf: conf[name] = dict()
				conf[name]['enabled'] = True
		for name in disabled:
			if name not in conf: conf[name] = dict()
			conf[name]['enabled'] = False
		if 'debug' not in conf_base: conf_base['debug'] = cfg.debug
		ep_conf[ep] = conf_base, conf, set()

	# Init global cfg for collectors/sinks' usage
	from graphite_metrics import collectors, sinks, loops
	collectors.cfg = sinks.cfg = loops.cfg = cfg

	# Init pluggable components
	import pkg_resources

	for ep_type in 'collector', 'sink':
		ep_key = '{}s'.format(ep_type) # a bit of a hack
		conf_base, conf, objects = ep_conf[ep_key]
		for ep in pkg_resources\
				.iter_entry_points('graphite_metrics.{}'.format(ep_key)):
			if ep.name[0] == '_':
				log.debug( 'Skipping {} enty point,'
					' prefixed by underscore: {}'.format(ep_type, ep.name) )
			subconf = conf.get(ep.name, conf_base)
			# Fill "_default" collector parameters
			for k,v in conf_base.viewitems():
				if k not in subconf: subconf[k] = v
			if subconf.get('enabled', True):
				log.debug('Loading {}: {}'.format(ep_type, ep.name))
				try: obj = getattr(ep.load(), ep_type)(subconf)
				except Exception as err:
					log.exception('Failed to load/init collector ({}): {}'.format(ep.name, err))
					subconf.enabled = False
				if subconf.get('enabled', True): objects.add(obj)
				else:
					log.debug(( '{} {} (entry point: {})'
						' was disabled after init' ).format(ep_type.title(), obj, ep.name))
		if not objects:
			log.fatal('No {}s were properly enabled/loaded, bailing out'.format(ep_type))
			sys.exit(1)
		log.debug('{}: {}'.format(ep_key.title(), objects))

	loop = dict( (ep.name, ep) for ep in
		pkg_resources.iter_entry_points('graphite_metrics.loops') )
	conf = AttrDict(**cfg.loop)
	if 'debug' not in conf: conf.debug = cfg.debug
	loop = loop[cfg.loop.name].load().loop(conf)

	collectors, sinks = it.imap( op.itemgetter(2),
		op.itemgetter('collectors', 'sinks')(ep_conf) )
	log.debug(
		'Starting main loop: {} ({} collectors, {} sinks)'\
		.format(loop, len(collectors), len(sinks)) )
	loop.start(collectors, sinks)

if __name__ == '__main__': main()
