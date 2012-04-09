#!/usr/bin/env python

{warning}

import itertools as it, operator as op, functools as ft
from glob import iglob
import os, sys

from setuptools import setup, find_packages

# Dirty workaround for "error: byte-compiling is disabled." message
sys.dont_write_bytecode = False

pkg_root = os.path.dirname(__file__)

setup(

	name = 'graphite-metrics',
	version = '{version}',
	author = 'Mike Kazantsev',
	author_email = 'mk.fraggod@gmail.com',
	license = 'WTFPL',
	keywords = 'graphite sysstat systemd cgroups metrics proc',
	url = 'http://github.com/mk-fg/graphite-metrics',

	description = 'Standalone Graphite metric data collectors for'
		' various stuff thats not (or poorly) handled by other monitoring daemons',
	long_description = open(os.path.join(pkg_root, 'README.md')).read(),

	classifiers = [
		'Development Status :: 4 - Beta',
		'Environment :: Other Environment',
		'Intended Audience :: Developers',
		'Operating System :: POSIX',
		'Operating System :: Unix',
		'Programming Language :: Python',
		'Topic :: Utilities',
		'Topic :: Internet :: Log Analysis',
		'Topic :: System :: Networking :: Monitoring'
		'License :: OSI Approved :: WTFPL' ],

	install_requires=['setuptools', 'xattr', 'iso8601'],

	packages = find_packages(),
	include_package_data = True,

	entry_points = {{
		'console_scripts': [
			'harvestd = graphite_metrics.harvestd:main',
			'sa_carbon = graphite_metrics.sa_carbon:main' ],
		'graphite_metrics.collectors': list(
			'{{0}} = graphite_metrics.collectors.{{0}}'.format(name[:-3])
			for name in it.imap(os.path.basename, iglob(os.path.join(
				pkg_root, 'graphite_metrics', 'collectors', '[!_]*.py' ))) ) }} )
