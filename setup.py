#!/usr/bin/env python

import itertools as it, operator as op, functools as ft
from glob import iglob
import os, sys

from setuptools import setup, find_packages

# Dirty workaround for "error: byte-compiling is disabled." message
sys.dont_write_bytecode = False

pkg_root = os.path.dirname(__file__)

setup(

	name = 'graphite-metrics',
	version = '12.04.33',
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

	install_requires=['setuptools', 'xattr', 'iso8601'],

	packages = find_packages(),
	include_package_data = True,

	package_data = {'graphite_metrics': ['harvestd.yaml']},
	entry_points = {
		'console_scripts': ['harvestd = graphite_metrics.harvestd:main'],
		'graphite_metrics.collectors': list(
			'{0} = graphite_metrics.collectors.{0}'.format(name[:-3])
			for name in it.imap(os.path.basename, iglob(os.path.join(
				pkg_root, 'graphite_metrics', 'collectors', '[!_]*.py' ))) ),
		'graphite_metrics.loops': list(
			'{0} = graphite_metrics.loops.{0}'.format(name[:-3])
			for name in it.imap(os.path.basename, iglob(os.path.join(
				pkg_root, 'graphite_metrics', 'loops', '[!_]*.py' ))) ),
		'graphite_metrics.sinks': list(
			'{0} = graphite_metrics.sinks.{0}'.format(name[:-3])
			for name in it.imap(os.path.basename, iglob(os.path.join(
				pkg_root, 'graphite_metrics', 'sinks', '[!_]*.py' ))) )
	} )
