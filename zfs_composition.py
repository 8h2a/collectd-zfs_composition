#!/usr/bin/env python3

"""
collectd-zfs_composition

A collectd plugin to visualize the used space (data and snapshots) for each ZFS pool.

It only calls the zpool/zfs binaries every CHECKINTERVAL seconds,
because some systems can't handle hammering these calls.

Inspired by https://github.com/schwerpunkt/munin-plugin-zfs_composition

Installation is the same as for any other collectd python plugin.
No settings necessary.

Requirements:
It might be required to set up a udev rule to grant permissions to /dev/zfs
for the user that runs this script.
Additionally an entry in /etc/sudoers or /etc/sudoers/.d/zfs might be
necessary to allow accessing the zfs/zpool binaries.
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
    print('{}: Reading data: {} / {} = {}'.format(
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


def read(data=None):
    global CHECKINTERVAL
    global last_check
    global stats

    # update data:
    if last_check + CHECKINTERVAL <= time.time():
        stats = execAndGetStdOut("/sbin/zfs list -Hp -o name,avail,used,usedsnap,usedds -t filesystem,volume")
        last_check = time.time()

    for (name, avail, used, usedsnap, usedds) in stats:
        poolName = name.split("/")[0]
        if "/" not in name:
            report(poolName, "Free", avail)
        report(poolName, '{}-{}'.format(name, "usedbydataset"), usedds)
        report(poolName, '{}-{}'.format(name, "usedbysnapshots"), usedsnap)


# collectd.register_read(read, INTERVAL)
collectd.register_read(read)
