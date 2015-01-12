#!/bin/sh
[ -f /tmp/ugrid.pid ] && kill $(cat /tmp/ugrid.pid) && rm /tmp/ugrid.pid
