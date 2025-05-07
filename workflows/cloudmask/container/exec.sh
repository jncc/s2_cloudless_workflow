#!/bin/bash

umask 002
# cd /working/software/workflows

export GDAL_DRIVER_PATH="/usr/lib/x86_64-linux-gnu/gdalplugins"
export GDAL_DATA="/usr/share/gdal"
export PROJ_DATA="/usr/local/share/proj"
export TMPDIR="/tmp"

PYTHONPATH='/working/software/workflows' luigi "$@"