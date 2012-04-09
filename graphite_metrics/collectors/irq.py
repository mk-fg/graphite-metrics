# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from io import open

from . import Collector, Datapoint

import logging
log = logging.getLogger(__name__)


class IRQ(Collector):

	@staticmethod
	def _parse_irq_table(table):
		irqs = dict()
		bindings = map(bytes.lower, table.readline().strip().split())
		bindings_cnt = len(bindings)
		for line in it.imap(bytes.strip, table):
			irq, line = line.split(None, 1)
			irq = irq.rstrip(':').lower()
			if irq in irqs:
				log.warn('Conflicting irq name/id: {!r}, skipping'.format(irq))
				continue
			irqs[irq] = map(int, line.split(None, bindings_cnt)[:bindings_cnt])
		return bindings, irqs

	def read(self):
		irq_tables = list()
		# /proc/interrupts
		with open('/proc/interrupts', 'rb') as table:
			irq_tables.append(self._parse_irq_table(table))
		# /proc/softirqs
		with open('/proc/softirqs', 'rb') as table:
			irq_tables.append(self._parse_irq_table(table))
		# dispatch
		for bindings, irqs in irq_tables:
			for irq, counts in irqs.viewitems():
				if sum(counts) == 0: continue
				for bind, count in it.izip(bindings, counts):
					yield Datapoint('irq.{}.{}'.format(irq, bind), 'counter', count, None)


collector = IRQ
