import json
import logging
import luigi
import os

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
    tempFolder = luigi.Parameter()
    templateFolder = luigi.Parameter()
    templateFilename = luigi.Parameter()
    s2CloudmaskContainer = luigi.Parameter()

    testProcessing = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
            output = input

        with open(Path(self.templateFolder).joinpath(self.templateFilename), 'r') as templateSBatch:
            sbatchTemplate = Template(templateSBatch.read())

        tasks = []
        for job in input['toProcess']:
            log.info(job)
            sbatch = sbatchTemplate.substitute({
                'workingMount': job['workingFolder'],
                'stateMount': job['stateFolder'],
                'inputMount': str(Path(job['inputPath']).parent),
                'outputMount': job['outputFolder'],
                's2CloudmaskContainer': self.s2CloudmaskContainer,
                'inputPath': '/input/' + Path(job['inputPath']).name,
                'bufferData': str(job['bufferData']),
                'bufferDistance': str(job['bufferDistance']),
                'reproject': str(job['reproject']),
                'reprojectionEPSG': str(job['reprojectionEPSG']),
                'keepIntermediates': str(job['keepIntermediates'])
            })

            sbatchScriptPath = Path(job['workingFolder']).joinpath(job['productName'] + '.sbatch')

            with open(sbatchScriptPath, 'w') as jobSBatch:
                jobSBatch.write(sbatch)

            task = SubmitJob(
                stateFolder = self.stateFolder,
                name =  job['productName'],
                sbatchScriptPath = f'{sbatchScriptPath}',
                testProcessing = self.testProcessing
            )

            tasks.append(task)

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'PrepareWorkingDirectories.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
