#!/bin/bash

ORCHESTRATION_BASEPATH="/gws/smf/j04/defra_eo/test/s2_cloudless_workflow/orchestration"
ORCHESTRATION_VENV="/gws/smf/j04/defra_eo/test/s2_cloudless_workflow/orchestration/processCedaArchive/.venv/bin/activate"
OUTPUT_BASEPATH="/gws/nopw/j04/defra_eo/data/output/sentinel/2/products/cloud"
WORKFLOW_CONTAINER="/gws/nopw/j04/defra_eo/test/containers/s2-cloudmasking-0.0.11.sif"

module load jaspy
source $ORCHESTRATION_VENV

cd $ORCHESTRATION_BASEPATH

PYTHONPATH='.' LUIGI_CONFIG_PATH='./luigi.cfg' luigi --module processCedaArchive SubmitJobs --stateFolder=@1/state/ --tempFolder=@1/temp/ --inputFolder=@1/input --outputFolder=$OUTPUT_BASEPATH/output/ --jobStateFolder=@1/job-state  --startDate=@2 --endDate=@3 --spatialOperator intersects --s2CloudmaskContainer=$WORKFLOW_CONTAINER --local-scheduler