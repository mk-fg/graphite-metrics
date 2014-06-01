#!/usr/bin/env python

import itertools as it, operator as op, functools as ft
from glob import iglob
import os, sys

from setuptools import setup, find_packages

pkg_root = os.path.dirname(__file__)

entry_points = dict(console_scripts=['harvestd = graphite_metrics.harvestd:main'])
entry_points.update(
	('graphite_metrics.{}'.format(ep_type), list(
		'{0} = graphite_metrics.{1}.{0}'\
			.format(os.path.basename(fn)[:-3], ep_type)
		for fn in iglob(os.path.join(
			pkg_root, 'graphite_metrics', ep_type, '[!_]*.py' )) ))
	for ep_type in ['collectors', 'processors', 'sinks', 'loops'] )

# Error-handling here is to allow package to be built w/o README included
try: readme = open(os.path.join(pkg_root, 'README.txt')).read()
except IOError: readme = ''

setup(

	name = 'graphite-metrics',
	version = '14.06.2',
	author = 'Mike Kazantsev',
	author_email = 'mk.fraggod@gmail.com',
	license = 'WTFPL',
	keywords = 'graphite sysstat systemd cgroups metrics proc',
	url = 'http://github.com/mk-fg/graphite-metrics',

	description = 'Standalone Graphite metric data collectors for'
		' various stuff thats not (or poorly) handled by other monitoring daemons',
	long_description = readme,

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

	install_requires = ['layered-yaml-attrdict-config', 'setuptools'],
	extras_require = {
		'collectors.cgacct': ['dbus-python'],
		'collectors.cron_log': ['xattr', 'iso8601'],
		'collectors.sysstat': ['xattr'],
		'collectors.cjdns_peer_traffic': ['bencode'],
		'sinks.librato_metrics': ['requests'],
		'sinks.librato_metrics.async': ['gevent'] },

	packages = find_packages(),
	package_data = {'': ['README.txt'], 'graphite_metrics': ['harvestd.yaml']},
	exclude_package_data = {'': ['README.*']},

	entry_points = entry_points )
