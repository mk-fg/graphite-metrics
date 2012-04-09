#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time, sleep
import os, sys, socket

import logging
log = logging.getLogger(__name__)


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

	import pkg_resources
	collectors = list( ep.load().collector() for ep in
		pkg_resources.iter_entry_points('graphite_metrics.collectors') )
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
