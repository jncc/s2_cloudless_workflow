import json
import logging
import luigi
import os

from datetime import datetime
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path
from string import Template

from processCedaArchive.PrepareWorkingDirectories import PrepareWorkingDirectories
from processCedaArchive.SubmitJob import SubmitJob

log = logging.getLogger('luigi-interface')

@requires(PrepareWorkingDirectories)
class SubmitJobs(luigi.Task):
    stateFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    workingFolder = luigi.Parameter()
    templateFolder = luigi.Parameter()
    templateFilename = luigi.Parameter()
    s2CloudmaskContainer = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default=False)

    slurmAccount = luigi.Parameter()

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
            output = input

        with open(Path(self.templateFolder).joinpath(self.templateFilename), 'r') as templateSBatch:
            sbatchTemplate = Template(templateSBatch.read())

        tasks = []

        for job in input['toProcess']:
            buffer = ''
            reproject = ''
            dataMounts = ''
            luigiTarget = 'CleanupTemporaryFiles'
            endingStatefilePath = f'{Path(job["stateFolder"]).joinpath(f"{luigiTarget}.json")}'

            if job['bufferData']:
                buffer = f'--bufferData --bufferDistance={str(job["bufferDistance"])}'
            if job['reproject']:
                reproject = f'--reproject --reprojectionEPSG={str(job["reprojectionEPSG"])}'
            if job['dataMounts']:
                for mount in job['dataMounts'].split(','):
                    dataMounts = f'{dataMounts} --bind {mount}:{mount}'

            sbatch = sbatchTemplate.substitute({
                'jobName': f'{Path(job["inputPath"]).name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
                'account': self.slurmAccount,
                'dataMounts': dataMounts,
                'workingMount': job['workingFolder'],
                'tmpMount': job['tmpFolder'],
                'stateMount': job['stateFolder'],
                'inputMount': job['inputFolder'],
                'outputMount': job['outputFolder'],
                's2CloudmaskContainer': self.s2CloudmaskContainer,
                'luigiTarget': luigiTarget,
                'inputPath': Path(job['inputPath']).name,
                'buffer': buffer,
                'reproject': reproject,
                'keepIntermediates': str(job['keepIntermediates']),
                'endingStatefilePath': endingStatefilePath
            })

            sbatchScriptPath = Path(job['workspaceFolder']).joinpath(job['productName'] + '.sbatch')

            with open(sbatchScriptPath, 'w') as jobSBatch:
                jobSBatch.write(sbatch)

            task = SubmitJob(
                stateFolder = self.stateFolder,
                name =  job['productName'],
                sbatchScriptPath = f'{sbatchScriptPath}',
                testProcessing = self.testProcessing
            )

            tasks.append(task)

        yield tasks

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'PrepareWorkingDirectories.json')
        log.error(infile)
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
