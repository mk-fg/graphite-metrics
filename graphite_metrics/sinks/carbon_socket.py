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
		host, port = self.conf.host
		reconnects = self.conf.max_reconnects
		while True:
			try:
				try:
					addrinfo = list(reversed(socket.getaddrinfo(
						host, port, socket.AF_UNSPEC, socket.SOCK_STREAM )))
				except socket.error as err:
					raise socket.gaierror(err.message)
				assert addrinfo, addrinfo
				while addrinfo:
					# Try connecting to all of the returned addresses
					af, socktype, proto, canonname, sa = addrinfo.pop()
					try:
						self.sock = socket.socket(af, socktype, proto)
						self.sock.connect(sa)
					except socket.error:
						if not addrinfo: raise
				log.debug('Connected to Carbon at {}:{}'.format(*sa))
				if send: self.sock.sendall(send)

			except (socket.error, socket.gaierror) as err:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				if isinstance(err, socket.gaierror):
					log.info('Failed to resolve host ({!r}): {}'.format(host, err))
				else: log.info('Failed to connect to {}:{}: {}'.format(host, port, err))
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
			self.reconnect(send=packet)


sink = CarbonSocket
