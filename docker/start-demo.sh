#!/bin/sh

sudo service ssh start
cd /home/ugrid/ugrid && exec ./bin/ugrid.js -l 0
