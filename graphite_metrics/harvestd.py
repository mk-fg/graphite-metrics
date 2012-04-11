#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time, sleep
from collections import Mapping
import os, sys, socket



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



def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect and dispatch various metrics to carbon daemon.')
	parser.add_argument('-t', '--carbon', metavar='host[:port]',
		help='host[:port] (default port: 2003, can be overidden'
			' via config file) of carbon tcp line-receiver destination.')
	parser.add_argument('-i', '--interval', type=int, metavar='seconds',
		help='Interval between collecting and sending the datapoints.')

	parser.add_argument('-e', '--enable',
		action='append', metavar='collector', default=list(),
		help='Enable only the specified metric collectors,'
				' can be specified multiple times.')
	parser.add_argument('-d', '--disable',
		action='append', metavar='collector', default=list(),
		help='Explicitly disable specified metric collectors,'
			' can be specified multiple times. Overrides --enabled.')

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

	# Logging
	import logging
	global log # will be initialized with default level otherwise
	configure_logging( cfg.logging,
		logging.DEBUG if optz.debug else logging.WARNING )
	logging.captureWarnings(cfg.logging.warnings)
	log = logging.getLogger(__name__)

	# Fill "auto-detected" blanks in the configuration, CLI overrides
	if not cfg.core.hostname:
		cfg.core.hostname = os.uname()[1]
	if optz.carbon: cfg.carbon.host = optz.carbon
	cfg.carbon.host = cfg.carbon.host.rsplit(':', 1)
	if len(cfg.carbon.host) == 1:
		cfg.carbon.host = cfg.carbon.host[0], cfg.carbon.default_port
	else: cfg.carbon.host[1] = int(cfg.carbon.host[1])
	if optz.interval: cfg.core.interval = optz.interval
	if optz.dry_run: cfg.debug.dry_run = optz.dry_run
	if optz.debug_data: cfg.debug.dump = optz.debug_data

	# Override "enabled" parameters, based on CLI
	conf_base = cfg.collectors._default
	if optz.enable:
		for name, conf in cfg.collectors.viewitems():
			if name not in optz.enable: conf['enabled'] = False
		conf_base['enabled'] = False
		for name in optz.enable:
			if name not in cfg.collectors: cfg.collectors[name] = dict()
			cfg.collectors[name]['enabled'] = True
	for name in optz.disable:
		if name not in cfg.collectors: cfg.collectors[name] = dict()
		cfg.collectors[name]['enabled'] = False
	if 'debug' not in conf_base: conf_base['debug'] = cfg.debug

	# Init global cfg for collectors' usage
	from graphite_metrics import collectors
	collectors.cfg = cfg

	# Init collectors
	import pkg_resources
	collectors = set()
	for ep in pkg_resources.iter_entry_points('graphite_metrics.collectors'):
		if ep.name[0] == '_':
			log.debug('Skipping enty point, prefixed by underscore: {}'.format(ep.name))
		conf = cfg.collectors.get(ep.name, conf_base)
		# Fill "_default" collector parameters
		for k,v in conf_base.viewitems():
			if k not in conf: conf[k] = v
		if conf.get('enabled', True):
			log.debug('Loading collector: {}'.format(ep.name))
			collector = ep.load().collector(conf)
			if conf.get('enabled', True): collectors.add(collector)
			else:
				log.debug(( 'Collector {} (entry point: {})'
					' has disabled itself after init' ).format(collector, ep.name))
	log.debug('Collectors: {}'.format(collectors))

	# Carbon connection init
	if not cfg.debug.dry_run:
		link = CarbonAggregator(
			cfg.core.hostname, cfg.carbon.host,
			max_reconnects=cfg.carbon.max_reconnects,
			reconnect_delay=cfg.carbon.reconnect_delay )

	ts = time()
	while True:
		data = list()
		for collector in collectors:
			log.debug('Polling data from a collector: {}'.format(collector))
			data.extend(collector.read())
		ts_now = time()

		log.debug('Sending {} datapoints'.format(len(data)))
		if cfg.debug.dump:
			for dp in data:
				dp = dp.get(ts=ts_now)
				if dp is None: continue
				name, value, ts_dp = dp
				log.info('Datapoint: {} {} {}'.format(
					'{}.{}'.format(cfg.core.hostname, name), value, int(ts_dp) ))
		if not cfg.debug.dry_run: link.send(ts_now, *data)

		while ts < ts_now: ts += cfg.core.interval
		ts_sleep = max(0, ts - time())
		log.debug('Sleep: {}s'.format(ts_sleep))
		sleep(ts_sleep)

if __name__ == '__main__': main()
