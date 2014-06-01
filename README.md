graphite-metrics: metric collectors for various stuff not (or poorly) handled by other monitoring daemons
--------------------

Core of the project is a simple daemon (harvestd), which collects metric values
and sends them to graphite carbon daemon (and/or other configured destinations)
once per interval.

Includes separate data collection components ("collectors") for processing of:

* /proc/slabinfo for useful-to-watch values, not everything (configurable).
* /proc/vmstat and /proc/meminfo in a consistent way.
* /proc/stat for irq, softirq, forks.
* /proc/buddyinfo and /proc/pagetypeinfo (memory fragmentation).
* /proc/interrupts and /proc/softirqs.
* Cron log to produce start/finish events and duration for each job into a
	separate metrics, adapts jobs to metric names with regexes.
* Per-system-service accounting using
	[systemd](http://www.freedesktop.org/wiki/Software/systemd) and it's cgroups
	("Default...Accounting=" options in system.conf have to be enabled for more
	recent versions).
* [sysstat](http://sebastien.godard.pagesperso-orange.fr/) data from sadc logs
	(use something like `sadc -F -L -S DISK -S XDISK -S POWER 60` to have more
	stuff logged there) via sadf binary and it's json export (`sadf -j`, supported
	since sysstat-10.0.something, iirc).
* iptables rule "hits" packet and byte counters, taken from ip{,6}tables-save,
	mapped via separate "table chain_name rule_no metric_name" file, which should
	be generated along with firewall rules (I use [this
	script](https://github.com/mk-fg/trilobite) to do that).

Additional metric collectors can be added via setuptools/distribute
graphite_metrics.collectors [entry
point](http://packages.python.org/distribute/setuptools.html?highlight=entry%20points#dynamic-discovery-of-services-and-plugins)
and confgured via the common configuration mechanism.

Same for the datapoint sinks (destinations - it doesn't have to be a single
carbon host), datapoint processors (mangle/rename/filter datapoints) and the
main loop, which can be replaced with the async (simple case - threads or
gevent) or buffering loop.

Currently supported backends (data destinations, sinks):

* [graphite carbon
	daemon](http://graphite.readthedocs.org/en/latest/carbon-daemons.html)
	(enabled/used by default)
* [librato metrics](https://metrics.librato.com/)

Look at the shipped collectors, processors, sinks and loops and their base
classes (like
[graphite_metrics.sinks.Sink](https://github.com/mk-fg/graphite-metrics/blob/master/graphite_metrics/sinks/__init__.py)
or
[loops.Basic](https://github.com/mk-fg/graphite-metrics/blob/master/graphite_metrics/loops/basic.py))
for API examples.


Installation
--------------------

It's a regular package for Python 2.7 (not 3.X).

Using [pip](http://pip-installer.org/) is the best way:

	% pip install graphite-metrics

If you don't have it, use:

	% easy_install pip
	% pip install graphite-metrics

Alternatively ([see
also](http://www.pip-installer.org/en/latest/installing.html)):

	% curl https://raw.github.com/pypa/pip/master/contrib/get-pip.py | python
	% pip install graphite-metrics

Or, if you absolutely must:

	% easy_install graphite-metrics

But, you really shouldn't do that.

Current-git version can be installed like this:

	% pip install 'git+https://github.com/mk-fg/graphite-metrics.git#egg=graphite-metrics'

### Requirements

Basic requirements are (pip or easy_install should handle these for you):

* [setuptools / distribute](https://pypi.python.org/pypi/distribute/) (for entry points)
* [layered-yaml-attrdict-config](https://pypi.python.org/pypi/layered-yaml-attrdict-config/)

Some shipped modules require additional packages to function (which can be
installed automatically by specifying extras on install, example: `pip install
'graphite-metrics[collectors.cgacct]'`):

* collectors

	* cgacct
		* [dbus-python](https://pypi.python.org/pypi/dbus-python/)

	* cron_log
		* [xattr](http://pypi.python.org/pypi/xattr/) (unless --xattr-emulation is used)
		* [iso8601](http://pypi.python.org/pypi/iso8601/)

	* sysstat
		* [xattr](http://pypi.python.org/pypi/xattr/) (unless --xattr-emulation is used)
		* (optional) [simplejson](http://pypi.python.org/pypi/simplejson/) - for
			better performance than stdlib json module

* sinks

	* librato_metrics
		* [requests](http://pypi.python.org/pypi/requests/)
		* (optional) [simplejson](http://pypi.python.org/pypi/simplejson/) - for
			better performance than stdlib json module
		* (optional) [gevent](http://pypi.python.org/pypi/gevent/) - to enable
			constant-time (more scalable) async submissions of large data chunks via
			concurrent API requests

Also see
[requirements.txt](https://github.com/mk-fg/graphite-metrics/blob/master/requirements.txt)
file or "install_requires" and "extras_require" in
[setup.py](https://github.com/mk-fg/graphite-metrics/blob/master/setup.py).


Running
--------------------

First run should probably look like this:

	% harvestd --debug -s dump -i10

That will use default configuration with all the collectors enabled, dumping
data to stderr (only "dump" data-sink enabled) and using short (5s) interval
between collected datapoints, dumpng additional info about what's being done.

After that, see [default harvestd.yaml configuration
file](https://github.com/mk-fg/graphite-metrics/blob/master/graphite_metrics/harvestd.yaml),
which contains configuration for all loaded collectors and can/should be
overidden using -c option.

Note that you don't have to specify all the options in each override-config,
just the ones you need to update.

For example, simple configuration file (say, /etc/harvestd.yaml) just to specify
carbon host and log lines format (dropping timestamp, since it will be piped to
syslog or systemd-journal anyway) might look like this:

	sinks:
	  carbon_socket:
	    host: carbon.example.host

	logging:
	  formatters:
	    basic:
	      format: '%(levelname)s :: %(name)s: %(message)s'

And be started like this: `harvestd -c /etc/harvestd.yaml`

See `harvestd --help` output for a full CLI reference.


Caveats, Stern Warnings and Apocalyptic Prophecies
--------------------

While most stock collectors here pull metrics from /proc once per some interval,
same as the other tools, be especially wary of the ones that process memory
metrics, like /proc/slabinfo and cgroup value parsers.

So-called "files" in /proc are actually callbacks in the kernel code, and to get
consistent reading for the whole slabinfo table, (at least some versions) of the
kernel have to lock some operations, causing unexpected lags and delays on the
whole system under some workloads (e.g. memcache servers).

cgroup data collector processes lots of files, potentially dozens, hundreds or
even thoursands per collection cycle, which may also cause similar issues.

Special thanks to Marcus Barczak for pointing that out.


Rationale
--------------------

Most other tools can (in theory) collect this data, and I've used
[collectd](http://collectd.org) for most of these, but it:

* Doesn't provide some of the most useful stuff - nfs stats, disk utilization
	time percentage, etc.

* Fails to collect some other stats, producing strange values like 0'es,
	unrealistic or negative values (for io, network, sensors, ...).

* General-purpose plugins like "tail" add lot of complexity, making
	configuration into a mess, while still lacking some basic functionality which
	10 lines of code (plugin) can easily provide (support is there, but see
	below).

* Plugins change metric names from the ones provided by /proc, referenced in
	kernel Documentation and on the internets, making collected data unnecessary
	hard to interpret and raising questions about it's meaning (which is
	increasingly important for low-level and calculated metrics).

Initially I've tried to address these issues (implement the same collectors)
with collectd plugins, but it's python plugin system turned out to be leaking
RAM and collectd itself segfaults something like once-a-day, even in the latest
releases, although probably because of issues in C plugins.

Plus, collectd data requires post-processing anyway - proper metric namespaces,
counter handling, etc.

Given that the alternative is to just get the data and echo it as "name val
timestamp" to tcp socket, decided to avoid the extra complexity and problems
that collectd provides.

Other than collectd, I've experimented with
[ganglia](http://ganglia.sourceforge.net/),
[munin](http://munin-monitoring.org/), and some other monitoring
infrastructures, but found little justification in re-using their aggregation
and/or collection infrastructure, if not outright limitations (like static data
schema in ganglia).

Daemon binary is (weirdly) called "harvestd" because "metricsd" name is already
used to refer to [another related daemon](https://github.com/kpumuk/metricsd)
(also, [there's a "metrics" w/o "d"](https://github.com/codahale/metrics),
probably others), and is too generic to be used w/o extra confusion, I think.
That, and I seem to lack creativity to come up with a saner name ("reaperd"
sounds too MassEffect'ish these days).
