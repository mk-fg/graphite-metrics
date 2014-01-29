# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft

from . import Sink

import logging
log = logging.getLogger(__name__)


class Dumper(Sink):

	'Just dumps the data to log. Useful for debugging.'

	def dispatch(self, *tuples):
		log.info('--- dump of {} datapoints'.format(len(tuples)))
		for name, value, ts_dp in tuples:
			log.info('Datapoint: {} {} {}'.format(name, value, ts_dp))
		log.info('--- dump end')


sink = Dumper
