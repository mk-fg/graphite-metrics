# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from io import open

from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


class Stats(Collector):

	def read(self):
		with open('/proc/stat', 'rb') as table:
			for line in table:
				label, vals = line.split(None, 1)
				total = int(vals.split(None, 1)[0])
				if label == 'intr': name = 'irq.total.hard'
				elif label == 'softirq': name = 'irq.total.soft'
				elif label == 'processes': name = 'processes.forks'
				else: continue # no more useful data here
				yield Datapoint(name, 'counter', total, None)


collector = Stats
