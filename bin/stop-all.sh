#!/bin/sh

# Compute canonical path to retrieve all conf/exec files from it
[ ${0%${0#?}} = / ] && cpath=$0 || cpath=$PWD/$0; cpath=$(cd "${cpath%/*}/.." && pwd)

[ -f "$cpath/conf/ugrid-env.sh" ] && . "$cpath/conf/ugrid-env.sh"

# Stop ugrid server
ugrid_cmd='test -f /tmp/ugrid.pid && kill $(cat /tmp/ugrid.pid); rm -f /tmp/ugrid.pid'
ssh ${UGRID_HOST:-localhost} "$ugrid_cmd"
