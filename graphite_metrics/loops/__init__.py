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

	def __init__(self, conf, time_func=time):
		self.conf, self.time_func = conf, time_func

	def start(self, collectors, processors, sinks):
		raise NotImplementedError( 'Loop.start method should be'
			' overidden in loop subclasses to start poll/process/send loop'
			' using passed Collector, Processor and Sink objects.' )
