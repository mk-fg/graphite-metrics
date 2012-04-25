# -*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
import os

from . import Processor

import logging
log = logging.getLogger(__name__)


class HostnamePrefix(Processor):

	'Adds a hostname as a prefix to metric name.'

	def __init__(self, *argz, **kwz):
		super(HostnamePrefix, self).__init__(*argz, **kwz)
		self.prefix = self.conf.hostname
		if self.prefix is None: self.prefix = os.uname()[1]
		if not self.prefix.endswith('.'): self.prefix += '.'

	def process(self, dp_tuple, sinks):
		name, value, ts_dp = dp_tuple
		return (self.prefix + name, value, ts_dp), sinks


processor = HostnamePrefix
