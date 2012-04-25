# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft

import logging
log = logging.getLogger(__name__)


class Processor(object):

	def __init__(self, conf):
		self.conf = conf

	def process(self, dp_tuple, sinks):
		raise NotImplementedError( 'Processor.process method'
			' should be overidden in processor subclasses to mangle'
			' (name, value, timestamp) tuple in some way.' )
