#!/bin/sh

yell() { echo "$0: $*" >&2; }
die() { yell "$*"; exit 111; }
try() { "$@" || die "cannot $*"; }

trap "rm -rf $PWD/t1" EXIT

try ./skale.js create t1 
try cd t1
try ../skale.js test
