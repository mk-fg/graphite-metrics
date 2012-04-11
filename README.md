graphite-metrics: standalone graphite collectors for various stuff not (or poorly) handled by other monitoring daemons
--------------------

Core of the project is a simple daemon (harvestd), which collects metric values
and sends them to graphite once per interval.

Consists of separate components ("collectors") for processing of:

* /proc/slabinfo for useful-to-watch values, not everything.
* /proc/vmstat and /proc/meminfo in a consistent way.
* /proc/stat for irq, softirq, forks.
* /proc/buddyinfo and /proc/pagetypeinfo (memory fragmentation).
* /proc/interrupts and /proc/softirqs.
* Cron log to produce start/finish events and duration for each job into a
	separate metrics, adapts jobs to metric names with regexes.
* Per-system-service accounting using
	[systemd](http://www.freedesktop.org/wiki/Software/systemd) and it's cgroups.
* [sysstat](http://sebastien.godard.pagesperso-orange.fr/) data from sadc logs
	(use something like `sadc -F -L -S DISK -S XDISK -S POWER 60` to have more
	stuff logged there) via sadf binary and it's json export (`sadf -j`, supported
	since sysstat-10.0.something, iirc).

Additional metric collectors can be added via setuptools
graphite_metrics.collectors entry point.
Look at shipped collectors for API examples.

	% harvestd -h
	usage: harvestd [-h] [-i INTERVAL] [-n] [--debug] remote

	Collect and dispatch various metrics to carbon daemon.

	positional arguments:
	  remote                host[:port] (default port: 2003) of carbon tcp line-
	                        receiver destination.

	optional arguments:
	  -h, --help            show this help message and exit
	  -i INTERVAL, --interval INTERVAL
	                        Interval between datapoints (default: 60).
	  -n, --dry-run         Do not actually send data.
	  --debug               Verbose operation mode.


Rationale
--------------------

Most other tools can (in theory) collect this data, and I've used
[collectd](http://collectd.org) for most of these, but it:

* Doesn't provide some of the most useful stuff - nfs stats, disk utilization
	time percentage, etc.

* Fails to collect some other stats, producing bullshit like 0'es,
	clearly-insane or negative values (for io, network, sensors, ...).

* General-purpose plugins like "tail" add lot of complexity, making
	configuration into a mess, while still lacking some basic functionality which
	10 lines of code can easily provide.

* Mangles names for metrics, as provided by /proc and referenced in kernel docs
	and on the internets, no idea what the hell for, "readability"?

Initially I've tried to implement these as collectd plugins, but it's python
plugin turned out to be leaking RAM and collectd itself segfaults something like
once-a-day, even in the latest releases (although probably because of bug in
some plugin).

Plus, collectd data requires post-processing anyway - proper metric namespaces,
counters, etc.

Given that the alternative is to just get the data and echo it as "name val
timestamp" to tcp socket, I just don't see why would I need all the extra
complexity and fail that collectd provides.

Other than collectd, I've experimented with
[ganglia](http://ganglia.sourceforge.net/), but it's static schema is a no-go
and most of stuff there doesn't make sense in graphite context.

Daemon binary is (weirdly) called "harvestd" because "metricsd" name is already
used to refer to [another graphite-related
daemon](https://github.com/kpumuk/metricsd) (also, [there is "metrics" w/o
"d"](https://github.com/codahale/metrics), probably others), and is too generic
to be used w/o extra confusion, I think.
That, and I seem to lack creativity to come up with a saner name ("reaperd"
sounds too MassEffect'ish these days).
