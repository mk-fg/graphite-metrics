from distutils.core import setup
from distutils.command.install import install
import os

from twisted.plugin import IPlugin, getPlugins

from distinctplugin import version

# If setuptools is present, use it to find_packages(), and also
# declare our dependency on epsilon.
extra_setup_args = {}
try:
	import setuptools
	from setuptools import find_packages
except ImportError:
	def find_packages():
		packages = []
		for directory, subdirectories, files in os.walk('systemd_cgacct'):
			if '__init__.py' in files: packages.append(directory.replace(os.sep, '.'))
		return packages

long_description = """
A plugin for txstatsd to report metrics from systemd cgroups.
"""


class TxPluginInstaller(install):
	def run(self):
		install.run(self)
		# Make sure we refresh the plugin list when installing, so we know
		# we have enough write permissions.
		# see http://twistedmatrix.com/documents/current/core/howto/plugin.html
		# "when installing or removing software which provides Twisted plugins,
		# the site administrator should be sure the cache is regenerated"
		list(getPlugins(IPlugin))

setup(
	cmdclass={'install': TxPluginInstaller},
	name='systemd_cgacct_plugin',
	version=version.distinctplugin,
	description='A txstatsd plugin for systemd cgroup metrics',
	author='Mike Kazantsev',
	url='https://github.com/mk-fg/txstatsd-systemd',
	license='MIT',
	packages=find_packages() + ['twisted.plugins'],
	long_description=long_description,
	classifiers=[
		'Development Status :: 4 - Beta',
		'Intended Audience :: Developers',
		'Intended Audience :: System Administrators',
		'Intended Audience :: Information Technology',
		'Programming Language :: Python',
		'Topic :: Database',
		'Topic :: Internet :: WWW/HTTP',
		'License :: OSI Approved :: MIT License' ],
	**extra_setup_args )
