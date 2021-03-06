# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from io import open
from hashlib import sha256, sha512
from base64 import b32decode
from collections import defaultdict
import os, sys, json, socket, time, types
from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


### For bencode bits below
# Derived from a thing under BitTorrent Open Source License, written by Petru Paler

# Different from vanilla bencode in:
#  * Handling "leading zeroes" in keys (doesn't error - for cjdns compat)
#  * encode_none method (to "n")
#  * encode_string encodes unicode as utf-8 bytes

def _ns_class(cls_name, cls_parents, cls_attrs):
	for k, v in cls_attrs.viewitems():
		if isinstance(v, types.FunctionType):
			cls_attrs[k] = classmethod(v)
	return type(cls_name, cls_parents, cls_attrs)

class BTEError(Exception): pass

class Bencached(object):
	__slots__ = 'bencoded',
	def __init__(self, s): self.bencoded = s

class BTE(object):
	__metaclass__ = _ns_class

	unicode_enc = 'utf-8'
	enable_none = False
	enable_bool = True
	cjdns_compat = True

	def decode_int(cls, x, f):
		f += 1
		newf = x.index('e', f)
		n = int(x[f:newf])
		if x[f] == '-':
			if x[f + 1] == '0': raise ValueError
		elif x[f] == '0' and newf != f+1: raise ValueError
		return n, newf+1
	def decode_string(cls, x, f):
		colon = x.index(':', f)
		n = int(x[f:colon])
		if not cls.cjdns_compat\
			and x[f] == '0' and colon != f+1: raise ValueError
		colon += 1
		return (x[colon:colon+n], colon+n)
	def decode_list(cls, x, f):
		r, f = [], f+1
		while x[f] != 'e':
			v, f = cls.decode_func[x[f]](cls, x, f)
			r.append(v)
		return r, f + 1
	def decode_dict(cls, x, f):
		r, f = {}, f+1
		while x[f] != 'e':
			k, f = cls.decode_string(x, f)
			r[k], f = cls.decode_func[x[f]](cls, x, f)
		return r, f + 1
	def decode_none(cls, x, f):
		if not cls.enable_none: raise ValueError(x[f])
		return None, f+1
	decode_func = dict(l=decode_list, d=decode_dict, i=decode_int, n=decode_none)
	for n in xrange(10): decode_func[bytes(n)] = decode_string

	def encode_bencached(cls, x, r): r.append(x.bencoded)
	def encode_int(cls, x, r): r.extend(('i', str(x), 'e'))
	def encode_float(cls, x, r): r.extend(('f', struct.pack('!d', x), 'e'))
	def encode_bool(cls, x, r):
		if not cls.enable_bool: raise ValueError(x)
		if x: cls.encode_int(1, r)
		else: cls.encode_int(0, r)
	def encode_string(cls, x, r):
		if isinstance(x, unicode):
			if not cls.unicode_enc: raise ValueError(x)
			x = x.encode(cls.unicode_enc)
		r.extend((str(len(x)), ':', x))
	def encode_list(cls, x, r):
		r.append('l')
		for i in x: cls.encode_func[type(i)](cls, i, r)
		r.append('e')
	def encode_dict(cls, x, r):
		r.append('d')
		ilist = x.items()
		ilist.sort()
		for k, v in ilist:
			r.extend((str(len(k)), ':', k))
			cls.encode_func[type(v)](cls, v, r)
		r.append('e')
	def encode_none(cls, x, r):
		if not cls.enable_none: raise ValueError(x)
		r.append('n')
	encode_func = {
		Bencached: encode_bencached,
		unicode: encode_string,
		str: encode_string,
		types.IntType: encode_int,
		types.LongType: encode_int,
		types.FloatType: encode_float,
		types.ListType: encode_list,
		types.TupleType: encode_list,
		types.DictType: encode_dict,
		types.BooleanType: encode_bool,
		types.NoneType: encode_none,
	}

	def bdecode(cls, x):
		try: r, l = cls.decode_func[x[0]](cls, x, 0)
		except (IndexError, KeyError, ValueError) as err:
			raise BTEError('Not a valid bencoded string: {}'.format(err))
		if l != len(x):
			raise BTEError('Invalid bencoded value (data after valid prefix)')
		return r

	def bencode(cls, x):
		r = []
		cls.encode_func[type(x)](cls, x, r)
		return ''.join(r)


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
			self.sock.send(BTE.bencode(dict(q='cookie')))
			cookie = BTE.bdecode(self.sock.recv(bs))['cookie']
		except Exception as err:
			raise PeerStatsFailure('Failed to get auth cookie', err)

		req = dict( q='auth',
			aq='InterfaceController_peerStats',
			args=dict(page=page),
			hash=sha256('{}{}'.format(password, cookie)).hexdigest(),
			cookie=cookie, txid=os.urandom(5).encode('hex') )
		req['hash'] = sha256(BTE.bencode(req)).hexdigest()

		try:
			self.sock.send(BTE.bencode(req))
			for n in xrange(self.conf.recv_retries + 1):
				resp = BTE.bdecode(self.sock.recv(bs))
				if resp.get('txid') != req['txid']: # likely timed-out responses to old requests
					log.warn('Received out-of-order response (n: %s, request: %s): %s', n, req, resp)
					continue
				return resp['peers'], resp.get('more', False)
		except Exception as err:
			raise PeerStatsFailure('Failure communicating with cjdns', err)
		raise PeerStatsFailure( 'Too many bogus (wrong or no txid) responses'
			' in a row (count: {}), last req/res: {} / {}'.format(self.conf.recv_retries, req, resp) )

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
