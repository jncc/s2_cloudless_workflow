import luigi
import logging
import subprocess
import json
import os
import random
import re
import datetime
from luigi import LocalTarget

log = logging.getLogger('luigi-interface')

class SubmitJob(luigi.Task):
    stateFolder = luigi.Parameter()
    name = luigi.Parameter()
    sbatchScriptPath = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)
    jobId = ""

    def run(self):
        try:
            outputFile = {
                "name": self.name,
                "sbatchScriptPath": self.sbatchScriptPath,
                "jobId": None,
                "submitTime": None
            }

            outputString = ""
            if self.testProcessing:
                randomJobId = random.randint(1000000,9999999)
                outputString = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"\
                                +str(randomJobId)+"   test001  RUN   short-serial jasmin-sci1 16*host290. my-job1 Nov 16 16:51"
            else:
                sbatchCmd = "sbatch {}".format(self.sbatchScriptPath)
                log.info("Submitting job using command: %s", sbatchCmd)
                output = subprocess.check_output(
                    sbatchCmd,
                    stderr=subprocess.STDOUT,
                    shell=True)
                outputString = output.decode("utf-8")

            regex = '[0-9]{5,}' # job ID is at least 5 digits
            match = re.search(regex, outputString)
            self.jobId = match.group(0)

            log.info("Successfully submitted slurm job <%s> for %s using sbatch script: %s", self.jobId, self.name, self.sbatchScriptPath)

            outputFile["jobId"] = self.jobId
            outputFile["submitTime"] = str(datetime.datetime.now())

            with self.output().open('w') as out:
                json.dump(outputFile, out, indent=4)

        except subprocess.CalledProcessError as e:
            errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)
        
    def output(self):
        outFile = os.path.join(self.stateFolder, f'SubmitJob_{self.name}_{self.jobId}.json')
        return LocalTarget(outFile)