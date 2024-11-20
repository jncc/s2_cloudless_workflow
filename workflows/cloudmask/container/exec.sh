#!/bin/bash

umask 002
cd /working/software/workflows
PYTHONPATH='.' luigi "$@"