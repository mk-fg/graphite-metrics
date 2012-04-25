# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import sleep
import socket

from . import Sink

import logging
log = logging.getLogger(__name__)


class CarbonSocket(Sink):

	'''Simple blocking non-buffering sender
		to graphite carbon tcp linereceiver interface.'''

	def __init__(self, conf):
		super(CarbonSocket, self).__init__(conf)
		if not self.conf.debug.dry_run: self.connect()

	def connect(self, send=None):
		reconnects = self.conf.max_reconnects
		while True:
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				self.sock.connect(self.conf.host)
				log.debug('Connected to Carbon at {}:{}'.format(*self.conf.host))
				if send: self.sock.sendall(send)
			except socket.error as err:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				log.info( 'Failed to connect to'
					' {0[0]}:{0[1]}: {1}'.format(self.conf.host, err) )
				if self.conf.reconnect_delay:
					sleep(max(0, self.conf.reconnect_delay))
			else: break

	def close(self):
		try: self.sock.close()
		except: pass

	def reconnect(self, send=None):
		self.close()
		self.connect(send=send)

	def dispatch(self, *tuples):
		reconnects = self.conf.max_reconnects
		packet = ''.join(it.starmap('{} {} {}\n'.format, tuples))
		try: self.sock.sendall(packet)
		except socket.error as err:
			log.error('Failed to send data to Carbon server: {}'.format(err))
			self.reconnect(send=send)


sink = CarbonSocket
