#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import itertools as it, operator as op, functools as ft
from contextlib import closing
from select import epoll, EPOLLIN, EPOLLOUT
from time import time, sleep
import os, sys, socket, struct, random, signal, re, logging


class LinkError(Exception): pass

class Pinger(object):

	@staticmethod
	def calculate_checksum(src):
		shift, src = sys.byteorder != 'little', bytearray(src)
		chksum = 0
		for c in src:
			chksum += (c << 8) if shift else c
			shift = not shift
		chksum = (chksum & 0xffff) + (chksum >> 16)
		chksum += chksum >> 16
		chksum = ~chksum & 0xffff
		return struct.pack('!H', socket.htons(chksum))

	@staticmethod
	def resolve(host, family=0, socktype=0, proto=0, flags=0):
		try: f, host = host.split(':', 1)
		except ValueError: pass
		else:
			if f == 'v4': family = socket.AF_INET
			elif f == 'v6':
				raise NotImplementedError('ICMPv6 pinging is not supported yet')
				family = socket.AF_INET6
		addrs = set( addr[-1][0] for addr in
			socket.getaddrinfo(host, 0, family, socktype, proto, flags) )
		return random.choice(list(addrs))


	def test_link(self, addr, ping_id=0xffff, seq=1):
		'Test if it is possible to send packets out at all (i.e. link is not down).'
		try: self.pkt_send(addr, ping_id, seq)
		except IOError as err: return False
		return True

	def pkt_send(self, dst, ping_id, seq):
		pkt = bytearray(struct.pack('!BBHHH', 8, 0, 0, ping_id, seq))
		pkt[2:4] = self.calculate_checksum(pkt)
		self.sock.sendto(bytes(pkt), (dst, 1))

	def pkt_recv(self):
		pkt, src = self.sock.recvfrom(2048)
		pkt = struct.unpack('!BBHHH', pkt[20:28])
		if pkt[0] != 0 or pkt[1] != 0: return
		return src[0], pkt[3], pkt[4] # ip, ping_id, seq


	def start(self, *args, **kws):
		with closing(socket.socket( socket.AF_INET,
				socket.SOCK_RAW, socket.getprotobyname('icmp') )) as self.sock:
			return self._start(*args, **kws)

	def _start( self, host_specs, interval,
			resolve_no_reply, resolve_fixed, ewma_factor, ping_pid, log=None,
			warn_tries=5, warn_repeat=None, warn_delay_k=5, warn_delay_min=5 ):
		ts = time()
		seq_gen = it.chain.from_iterable(it.imap(xrange, it.repeat(2**15)))
		resolve_fixed_deadline = ts + resolve_fixed
		resolve_retry = dict()
		self.discard_rtts = False
		if not log: log = logging.getLogger(__name__)

		### First resolve all hosts, waiting for it, if necessary
		hosts, host_ids = dict(), dict()
		for host in host_specs:
			while True:
				ping_id = random.randint(0, 0xfffe)
				if ping_id not in host_ids: break
			warn = warn_ts = 0
			while True:
				try:
					ip = self.resolve(host)
					if not self.test_link(ip): raise LinkError

				except (socket.gaierror, socket.error, LinkError) as err:
					ts = time()
					if warn < warn_tries:
						warn_force, warn_chk = False, True
					else:
						warn_force, warn_chk = True, warn_repeat\
							and (warn_repeat is True or ts - warn_ts > warn_repeat)
					if warn_chk: warn_ts = ts
					(log.warn if warn_chk else log.info)\
						( '{}Unable to resolve/send-to name spec: {}'\
							.format('' if not warn_force else '(STILL) ', host) )
					warn += 1
					if warn_repeat is not True and warn == warn_tries:
						log.warn( 'Disabling name-resolver/link-test warnings (failures: {},'
							' name spec: {}) until next successful attempt'.format(warn, host) )
					sleep(max(interval / float(warn_delay_k), warn_delay_min))

				else:
					hosts[host] = host_ids[ping_id] = dict(
						ping_id=ping_id, ip=ip,
						last_reply=0, rtt=0, sent=0, recv=0 )
					if warn >= warn_tries:
						log.warn('Was able to resolve host spec: {} (attempts: {})'.format(host, warn))
					break

		### Handler to emit results on-demand
		def dump(sig, frm):
			self.discard_rtts = True # make sure results won't be tainted by this delay
			ts = time()
			try:
				for spec, host in hosts.viewitems():
					sys.stdout.write('{} {:.10f} {:.10f} {:010d}\n'.format(
						spec, ts - host['last_reply'], host['rtt'],
						max(host['sent'] - host['recv'] - 1, 0) )) # 1 pkt can be in-transit
					if host['sent'] > 2**30: host['sent'] = host['recv'] = 0
				sys.stdout.write('\n')
				sys.stdout.flush()
			except IOError: sys.exit()
		signal.signal(signal.SIGQUIT, dump)

		### Actual ping-loop
		poller = epoll()
		poller.register(self.sock, EPOLLIN)
		sys.stdout.write('\n')
		sys.stdout.flush()

		ts_send = 0 # when last packet(s) were sent
		while True:
			while True:
				poll_time = max(0, ts_send + interval - time())
				try:
					poll_res = poller.poll(poll_time)
					if not poll_res or not poll_res[0][1] & EPOLLIN: break
					pkt = self.pkt_recv()
					if not pkt: continue
					ip, ping_id, seq = pkt
				except IOError: continue
				if not ts_send: continue
				ts = time()
				try: host = host_ids[ping_id]
				except KeyError: pass
				else:
					host['last_reply'] = ts
					host['recv'] += 1
					if not self.discard_rtts:
						host['rtt'] = host['rtt'] + ewma_factor * (ts - ts_send - host['rtt'])

			if resolve_retry:
				for spec, host in resolve_retry.items():
					try: host['ip'] = self.resolve(spec)
					except socket.gaierror as err:
						log.warn('Failed to resolve spec: {} (host: {}): {}'.format(spec, host, err))
						host['resolve_fails'] = host.get('resolve_fails', 0) + 1
						if host['resolve_fails'] >= warn_tries:
							log.error(( 'Failed to resolve host spec {} (host: {}) after {} attempts,'
								' exiting (so subprocess can be restarted)' ).format(spec, host, warn_tries))
							# More complex "retry until forever" logic is used on process start,
							#  so exit here should be performed only once per major (non-transient) failure
							sys.exit(0)
					else:
						host['resolve_fails'] = 0
						del resolve_retry[spec]

			if time() > resolve_fixed_deadline:
				for spec,host in hosts.viewitems():
					try: host['ip'] = self.resolve(spec)
					except socket.gaierror: resolve_retry[spec] = host
				resolve_fixed_deadline = ts + resolve_fixed

			if ping_pid:
				try: os.kill(ping_pid, 0)
				except OSError: sys.exit()

			resolve_reply_deadline = time() - resolve_no_reply
			self.discard_rtts, seq = False, next(seq_gen)
			for spec, host in hosts.viewitems():
				if host['last_reply'] < resolve_reply_deadline:
					try: host['ip'] = self.resolve(spec)
					except socket.gaierror: resolve_retry[spec] = host
				send_retries = 30
				while True:
					try: self.pkt_send(host['ip'], host['ping_id'], seq)
					except IOError as err:
						send_retries -= 1
						if send_retries == 0:
							log.error(( 'Failed sending pings from socket to host spec {}'
									' (host: {}) attempts ({}), killing pinger (so it can be restarted).' )\
								.format(spec, host, err))
							sys.exit(0) # same idea as with resolver errors abolve
						continue
					else: break
					host['sent'] += 1
			ts_send = time() # used to calculate rtt's


if __name__ == '__main__':
	signal.signal(signal.SIGQUIT, signal.SIG_IGN)
	logging.basicConfig()
	# Inputs
	Pinger().start( sys.argv[7:], interval=float(sys.argv[1]),
		resolve_no_reply=float(sys.argv[2]), resolve_fixed=float(sys.argv[3]),
		ewma_factor=float(sys.argv[4]), ping_pid=int(sys.argv[5]),
		warn_tries=int(sys.argv[6]), log=logging.getLogger('pinger'),
		warn_repeat=8 * 3600, warn_delay_k=5, warn_delay_min=5 )
	# Outputs on SIGQUIT: "host_spec time_since_last_reply rtt_median sent-recvd"
