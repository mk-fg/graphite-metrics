# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft

import logging
log = logging.getLogger(__name__)


class Sink(object):

	def __init__(self, conf):
		self.conf = conf

	def dispatch(self, *tuples):
		raise NotImplementedError( 'Sink.dispatch method should be overidden in sink'
			' subclasses to dispatch (metric_name, value, timestamp) tuples to whatever destination.' )
