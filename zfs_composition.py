#!/usr/bin/env python3

"""
collectd-zfs_composition

A collectd plugin to visualize the used space (data and snapshots) for each ZFS pool.

It only calls the zpool/zfs binaries every CHECKINTERVAL seconds, 
because some systems can't handle hammering these calls.

Inspired by https://github.com/schwerpunkt/munin-plugin-zfs_composition

Installation is the same as for any other collectd python plugin.
No settings necessary.
"""

import time
import subprocess
import collectd


PLUGIN_NAME = 'zfs_composition'
# interval in seconds, where new data gets fetched using the zfs/zpool commands
CHECKINTERVAL = 300
# INTERVAL = 5  # report interval

collectd.info('Loading Python plugin: ' + PLUGIN_NAME)


def execAndGetStdOut(cmd, splitTab=True):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    p_status = p.wait()
    if p_status != 0:
        raise Exception('Failed to execute {}'.format(cmd))
    output = output.decode("utf-8").splitlines()
    if splitTab:
        output = [x.split('\t') for x in output]
    return output


def report(plugin_instance, type_instance, value):
    global PLUGIN_NAME
    collectd.debug('{}: Reading data: {}/{} = {}'.format(
        PLUGIN_NAME, plugin_instance, type_instance, value))

    collectd.Values(
        type='gauge',
        plugin=PLUGIN_NAME,
        plugin_instance=plugin_instance.replace('/', '_'),
        type_instance=type_instance.replace('/', '_'),
        values=[value]).dispatch()


# for caching the output of the zfs/zpool commands:
last_check = 0.0
stats = []
pools = []


def read(data=None):
    global PLUGIN_NAME
    global CHECKINTERVAL
    global last_check
    global stats
    global pools

    # update data:
    if last_check + CHECKINTERVAL <= time.time():
        stats = execAndGetStdOut("sudo /sbin/zfs get -Hp usedbydataset,usedbysnapshots -r | grep -v @")
        pools = execAndGetStdOut("sudo /sbin/zpool get -Hp free")
        last_check = time.time()

    for (poolName, _, free, _) in pools:
        pool_stats = [x for x in stats if x[0].startswith(poolName)]
        for (dataset, key, value, _) in pool_stats:
            report(poolName, '{}-{}'.format(dataset, key), value)
        report(poolName, "Free", free)

# collectd.register_read(read, INTERVAL)
collectd.register_read(read)
