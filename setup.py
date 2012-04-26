#!/usr/bin/env python

import itertools as it, operator as op, functools as ft
from glob import iglob
import os, sys

from setuptools import setup, find_packages

# Dirty workaround for "error: byte-compiling is disabled." message
sys.dont_write_bytecode = False

pkg_root = os.path.dirname(__file__)

entry_points = dict(console_scripts=['harvestd = graphite_metrics.harvestd:main'])
for ep_type in 'collectors', 'processors', 'sinks', 'loops':
	entry_points['graphite_metrics.{}'.format(ep_type)] = list(
		'{0} = graphite_metrics.{1}.{0}'.format(name[:-3], ep_type)
		for name in it.imap(os.path.basename, iglob(os.path.join(
			pkg_root, 'graphite_metrics', ep_type, '[!_]*.py' ))) )

setup(

	name = 'graphite-metrics',
	version = '12.04.44',
	author = 'Mike Kazantsev',
	author_email = 'mk.fraggod@gmail.com',
	license = 'WTFPL',
	keywords = 'graphite sysstat systemd cgroups metrics proc',
	url = 'http://github.com/mk-fg/graphite-metrics',

	description = 'Standalone Graphite metric data collectors for'
		' various stuff thats not (or poorly) handled by other monitoring daemons',
	long_description = open(os.path.join(pkg_root, 'README.pypi')).read(),

	classifiers = [
		'Development Status :: 4 - Beta',
		'Environment :: No Input/Output (Daemon)',
		'Intended Audience :: Developers',
		'Intended Audience :: System Administrators',
		'Intended Audience :: Telecommunications Industry',
		'License :: OSI Approved',
		'Operating System :: POSIX',
		'Operating System :: Unix',
		'Programming Language :: Python',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 2 :: Only',
		'Topic :: Internet',
		'Topic :: Internet :: Log Analysis',
		'Topic :: System :: Monitoring',
		'Topic :: System :: Networking :: Monitoring',
		'Topic :: System :: Operating System Kernels :: Linux' ],

	install_requires = ['PyYAML', 'setuptools'],
	extras_require = {
		'collectors.cgacct': ['dbus-python'],
		'collectors.cron_log': ['xattr', 'iso8601'],
		'collectors.sysstat': ['xattr'],
		'sinks.librato_metrics': ['requests'] },

	packages = find_packages(),
	include_package_data = True,

	package_data = {'graphite_metrics': ['harvestd.yaml']},
	entry_points = entry_points )
