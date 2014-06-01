# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from io import open
from hashlib import sha256, sha512
from base64 import b32decode
from collections import defaultdict
import os, sys, json, socket, time, types
from . import Collector, Datapoint

from bencode import bencode, bdecode

import logging
log = logging.getLogger(__name__)


def pubkey_to_ipv6(key,
		_cjdns_b32_map = [ # directly from util/Base32.h
			99,99,99,99,99,99,99,99,99,99,99,99,99,99,99,99,
			99,99,99,99,99,99,99,99,99,99,99,99,99,99,99,99,
			99,99,99,99,99,99,99,99,99,99,99,99,99,99,99,99,
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9,99,99,99,99,99,99,
			99,99,10,11,12,99,13,14,15,99,16,17,18,19,20,99,
			21,22,23,24,25,26,27,28,29,30,31,99,99,99,99,99,
			99,99,10,11,12,99,13,14,15,99,16,17,18,19,20,99,
			21,22,23,24,25,26,27,28,29,30,31,99,99,99,99,99 ]):
	if key.endswith('.k'): key = key[:-2]

	bits, byte, res = 0, 0, list()
	for c in key:
		n = _cjdns_b32_map[ord(c)]
		if n > 31: raise ValueError('Invalid key: {!r}, char: {!r}'.format(key, n))
		byte |= n << bits
		bits += 5
		if bits >= 8:
			bits -= 8
			res.append(chr(byte & 0xff))
			byte >>= 8
	if bits >= 5 or byte:
		raise ValueError('Invalid key length: {!r} (leftover bits: {})'.format(key, bits))
	res = ''.join(res)

	addr = sha512(sha512(res).digest()).hexdigest()[:32]
	if addr[:2] != 'fc':
		raise ValueError( 'Invalid cjdns key (first'
			' addr byte is not 0xfc, addr: {!r}): {!r}'.format(addr, key) )
	return addr


class PeerStatsFailure(Exception):

	def __init__(self, msg, err=None):
		if err is not None: msg += ': {} {}'.format(type(err), err)
		super(PeerStatsFailure, self).__init__(msg)

	def __hash__(self):
		return hash(self.message)


class CjdnsPeerStats(Collector):

	last_err = None
	last_err_count = None # None (pre-init), True (shut-up mode) or int
	last_err_count_max = 3 # max repeated errors to report

	def __init__(self, *argz, **kwz):
		super(CjdnsPeerStats, self).__init__(*argz, **kwz)

		assert self.conf.filter.direction in\
			['any', 'incoming', 'outgoing'], self.conf.filter.direction

		if isinstance(self.conf.peer_id, types.StringTypes):
			self.conf.peer_id = [self.conf.peer_id]

		conf_admin, conf_admin_path = None,\
			os.path.expanduser(self.conf.cjdnsadmin_conf)
		try:
			with open(conf_admin_path) as src: conf_admin = json.load(src)
		except (OSError, IOError) as err:
			log.warn('Unable to open cjdnsadmin config: %s', err)
		except ValueError as err:
			log.warn('Unable to process cjdnsadmin config: %s', err)
		if conf_admin is None:
			log.error('Failed to process cjdnsadmin config, disabling collector')
			self.conf.enabled = False
			return

		sock_addr = conf_admin['addr'], conf_admin['port']
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.settimeout(self.conf.timeout)
		log.debug('Using cjdns socket: {}:{}'.format(*sock_addr))
		self.sock.connect(sock_addr)

		self.admin_password = conf_admin['password']
		self.peer_ipv6_cache = dict()

	def get_stats_page(self, page, password, bs=2**30):
		try:
			self.sock.send(bencode(dict(q='cookie')))
			cookie = bdecode(self.sock.recv(bs))['cookie']
		except Exception as err:
			raise PeerStatsFailure('Failed to get auth cookie', err)

		req = dict( q='auth',
			aq='InterfaceController_peerStats',
			args=dict(page=page),
			hash=sha256('{}{}'.format(password, cookie)).hexdigest(),
			cookie=cookie, txid=os.urandom(5).encode('hex') )
		req['hash'] = sha256(bencode(req)).hexdigest()

		try:
			self.sock.send(bencode(req))
			resp = bdecode(self.sock.recv(bs))
			assert resp.get('txid') == req['txid'], [req, resp]
			return resp['peers'], resp.get('more', False)
		except Exception as err:
			raise PeerStatsFailure('Failure communicating with cjdns', err)

	def get_peer_stats(self):
		peers, page, more = list(), 0, True
		while more:
			stats, more = self.get_stats_page(page, self.admin_password)
			peers.extend(stats)
			page += 1
		return peers

	def read(self):
		try: peers = self.get_peer_stats()
		# PeerStatsFailure errors' reporting is rate-limited
		except PeerStatsFailure as err:
			if hash(err) == hash(self.last_err):
				if self.last_err_count is True: return
				elif self.last_err_count < self.last_err_count_max: self.last_err_count += 1
				else:
					log.warn( 'Failed getting cjdns peer stats:'
						' {} -- disabling reporting of recurring errors'.format(err) )
					self.last_err_count = True
					return
			else: self.last_err, self.last_err_count = err, 1
			log.warn('Failed getting cjdns peer stats: {}'.format(err))
			return
		else:
			if self.last_err_count is True:
				log.warn('Previous recurring failure ({}) was resolved'.format(self.last_err))
				self.last_err = self.last_err_count = None

		# Detect peers with 2 links having different isIncoming
		peers_bidir = dict()
		for peer in peers:
			val = peers_bidir.get(peer['publicKey'])
			if val is False: peers_bidir[peer['publicKey']] = True
			elif val is None: peers_bidir[peer['publicKey']] = False

		ts, peer_states = time.time(), defaultdict(int)
		for peer in peers:
			state = peer['state'].lower()
			peer_states[state] += 1

			# Check filters
			if self.conf.filter.established_only and state != 'established': continue
			if self.conf.filter.direction != 'any':
				if self.conf.filter.direction == 'incoming' and not peer['isIncoming']: continue
				elif self.conf.filter.direction == 'outgoing' and peer['isIncoming']: continue
				else: raise ValueError(self.conf.filter.direction)

			# Generate metric name
			pubkey = peer['publicKey']
			if pubkey.endswith('.k'): pubkey = pubkey[:-2]
			peer['pubkey'] = pubkey
			if 'ipv6' in self.conf.peer_id:
				if pubkey not in self.peer_ipv6_cache:
					self.peer_ipv6_cache[pubkey] = pubkey_to_ipv6(pubkey)
				peer['ipv6'] = self.peer_ipv6_cache[pubkey]
			for k in self.conf.peer_id:
				if k in peer:
					peer_id = peer[k]
					break
			else: raise KeyError(self.conf.peer_id, peer)
			name = '{}.{}.{{}}'.format(self.conf.prefix, peer_id)
			if peers_bidir[peer['publicKey']]:
				name = name.format('incoming_{}' if peer['isIncoming'] else 'outgoing_{}')

			# Per-peer metrics
			name_bytes = name.format('bytes_{}')
			for k, d in [('bytesIn', 'in'), ('bytesOut', 'out')]:
				yield Datapoint(name_bytes.format(d), 'counter', peer[k], ts)
			if self.conf.special_metrics.peer_link:
				link = 1 if state == 'established' else 0
				yield Datapoint(name.format(self.conf.special_metrics.peer_link), 'gauge', link, ts)

		# Common metrics
		if self.conf.special_metrics.count:
			yield Datapoint(self.conf.special_metrics.count, 'gauge', len(peers), ts)
		if self.conf.special_metrics.count_state:
			for k, v in peer_states.viewitems():
				name = '{}.{}'.format(self.conf.special_metrics.count_state, k)
				yield Datapoint(name, 'gauge', v, ts)


collector = CjdnsPeerStats
