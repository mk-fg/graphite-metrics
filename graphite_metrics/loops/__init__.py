# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time

import logging
log = logging.getLogger(__name__)

# Global configuration for harvestd,
#  intended to be set before initializing loops,
#  but should not be really relied upon - can be empty.
cfg = dict()


class Loop(object):

	def __init__(self, conf, collectors, sinks, time_func=time):
		self.conf, self.time_func = conf, time_func
		self.collectors, self.sinks = collectors, sinks

	def start(self):
		raise NotImplementedError( 'Loop.start method should be'
			' overidden in loop subclasses to start poll/send loop using'
			' Collector and Sink objects (passed to init).' )
