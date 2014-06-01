# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from subprocess import Popen, PIPE
from io import open
import os, signal

from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


class PingerInterface(Collector):

	def __init__(self, *argz, **kwz):
		super(PingerInterface, self).__init__(*argz, **kwz)
		self.hosts = dict(it.chain(
			( ('v4:{}'.format(spec), name)
				for name, spec in (self.conf.hosts.ipv4 or dict()).viewitems() ),
			( ('v6:{}'.format(spec), name)
				for name, spec in (self.conf.hosts.ipv6 or dict()).viewitems() ) ))
		if not self.hosts:
			log.info('No valid hosts to ping specified, disabling collector')
			self.conf.enabled = False
		else: self.spawn_pinger()

	def spawn_pinger(self):
		cmd = (
			['python', os.path.join(os.path.dirname(__file__), '_ping.py')]
				+ map(bytes, [ self.conf.interval,
					self.conf.resolve.no_reply or 0, self.conf.resolve.time or 0,
					self.conf.ewma_factor, os.getpid(), self.conf.resolve.max_retries ])
				+ self.hosts.keys() )
		log.debug('Starting pinger subprocess: {}'.format(' '.join(cmd)))
		self.proc = Popen(cmd, stdout=PIPE)
		self.proc.stdout.readline() # wait until it's initialized

	def read(self):
		err = self.proc.poll()
		if err is not None:
			log.warn( 'Pinger subprocess has failed'
				' (exit code: {}), restarting it'.format(err) )
			self.spawn_pinger()
		else:
			self.proc.send_signal(signal.SIGQUIT)
			for line in iter(self.proc.stdout.readline, ''):
				line = line.strip()
				if not line: break
				host, ts_offset, rtt, lost = line.split()
				host = self.hosts[host]
				yield Datapoint('network.ping.{}.ping'.format(host), 'gauge', float(rtt), None)
				yield Datapoint('network.ping.{}.droprate'.format(host), 'counter', int(lost), None)


collector = PingerInterface
