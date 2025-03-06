#!/bin/bash

ORCHESTRATION_BASEPATH="./"
ORCHESTRATION_VENV="./.venv/bin/activate"
OUTPUT_BASEPATH="./output"
WORKFLOW_CONTAINER="./containers/s2-cloudmasking-0.0.12.sif"

module load jaspy
source $ORCHESTRATION_VENV

cd $ORCHESTRATION_BASEPATH

PYTHONPATH='.' LUIGI_CONFIG_PATH='./luigi.cfg' luigi --module processCedaArchive SubmitJobs --stateFolder=@1/state/ --workingFolder=@1/work/ --inputFolder=@1/input --outputFolder=$OUTPUT_BASEPATH/output/ --jobStateFolder=@1/job-state  --startDate=@2 --endDate=@3 --spatialOperator intersects --s2CloudmaskContainer=$WORKFLOW_CONTAINER --local-scheduler