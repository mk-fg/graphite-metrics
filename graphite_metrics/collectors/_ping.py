#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import itertools as it, operator as op, functools as ft
from contextlib import closing
from select import epoll, EPOLLIN, EPOLLOUT
from time import time, sleep
import os, sys, socket, struct, random, signal, re


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


	def pkt_send(self, sock, dst, ping_id, seq):
		pkt = bytearray(struct.pack('!BBHHH', 8, 0, 0, ping_id, seq))
		pkt[2:4] = self.calculate_checksum(pkt)
		sock.sendto(bytes(pkt), (dst, 1))

	def pkt_recv(self, sock):
		pkt, src = sock.recvfrom(2048)
		pkt = struct.unpack('!BBHHH', pkt[20:28])
		if pkt[0] != 0 or pkt[1] != 0: return
		return src[0], pkt[3], pkt[4] # ip, ping_id, seq


	def start( self, host_specs, interval,
			resolve_no_reply, resolve_fixed, ewma_factor, ping_pid ):
		ts = time()
		seq_gen = it.chain.from_iterable(it.imap(xrange, it.repeat(2**15)))
		resolve_fixed_deadline = ts + resolve_fixed
		self.discard_rtts = False

		hosts, host_ids = dict(), dict()
		for host in host_specs:
			while True:
				ping_id = random.randint(0, 0xffff)
				if ping_id not in hosts: break
			hosts[host] = host_ids[ping_id] = dict(
				ping_id=ping_id, ip=self.resolve(host),
				last_reply=0, rtt=0, sent=0, recv=0 )

		def dump(sig, frm):
			self.discard_rtts = True # make sure results won't be tainted by this delay
			ts = time()
			try:
				for spec, host in hosts.viewitems():
					sys.stdout.write('{} {} {} {}\n'.format(
						spec, ts - host['last_reply'], host['rtt'],
						max(host['sent'] - host['recv'] - 1, 0) )) # 1 pkt can be in-transit
					if host['sent'] > 2**30: host['sent'] = host['recv'] = 0
				sys.stdout.write('\n')
				sys.stdout.flush()
			except IOError: sys.exit()

		with closing(socket.socket( socket.AF_INET,
				socket.SOCK_RAW, socket.getprotobyname('icmp') )) as sock:
			poller = epoll()
			poller.register(sock, EPOLLIN)
			signal.signal(signal.SIGQUIT, dump)
			sys.stdout.write('\n')
			sys.stdout.flush()

			ts_send = time() - interval
			while True:
				ts = time()
				while True:
					poll_time = max(0, ts_send + interval - ts)
					try:
						poll_res = poller.poll(poll_time)
						if not poll_res or not poll_res[0][1] & EPOLLIN: break
						pkt = self.pkt_recv(sock)
						if not pkt: continue
						ip, ping_id, seq = pkt
					except IOError: continue
					ts = time()
					try: host = host_ids[ping_id]
					except KeyError: pass
					else:
						host['last_reply'] = ts
						host['recv'] += 1
						if not self.discard_rtts:
							host['rtt'] = host['rtt'] + ewma_factor * (ts - ts_send - host['rtt'])

				if ts > resolve_fixed_deadline:
					for spec,host in hosts.viewitems(): host['ip'] = self.resolve(spec)
					resolve_fixed_deadline = ts + resolve_fixed
				if ping_pid:
					try: os.kill(ping_pid, 0)
					except OSError: sys.exit()

				ts_send += interval
				resolve_reply_deadline = ts - resolve_no_reply
				self.discard_rtts, seq = False, next(seq_gen)
				for spec, host in hosts.viewitems():
					if host['last_reply'] < resolve_reply_deadline:
						host['ip'] = self.resolve(spec)
					while True:
						try: self.pkt_send(sock, host['ip'], host['ping_id'], seq)
						except IOError: continue
						else: break
						host['sent'] += 1


if __name__ == '__main__':
	signal.signal(signal.SIGQUIT, signal.SIG_IGN)
	Pinger().start( sys.argv[6:], interval=float(sys.argv[1]),
		resolve_no_reply=float(sys.argv[2]), resolve_fixed=float(sys.argv[3]),
		ewma_factor=float(sys.argv[4]), ping_pid=int(sys.argv[5]) )
