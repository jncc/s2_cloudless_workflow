import json
import logging
import luigi
import os

from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path
from string import Template

from PrepareWorkingDirectories import PrepareWorkingDirectories
from SubmitJob import SubmitJob

log = logging.getLogger('luigi-interface')

@requires(PrepareWorkingDirectories)
class SetupWorkingDirectories(luigi.Task):
    stateFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    templateFolder = luigi.Parameter()
    templateFilename = luigi.Parameter()

    def run(self):
        with input().open('r') as i:
            input = json.load(i)
            output = input

        with open(Path(self.templateFolder).joinpath(self.templateFilename), 'r') as templateSBatch:
            sbatchTemplate = Template(templateSBatch.read())

        tasks = []
        for job in input['toProcess']:
            sbatch = sbatchTemplate.substitute(self.getTemplateParams({
                'workingMount': job['workingFolder'],
                'stateMount': job['stateFolder'],
                'inputMount': str(Path(job['inputPath']).parent),
                'outputMount': job['outputFolder'],
                'inputPath': f'/input/{Path(job['inputPath']).name}',
                'bufferData': str(job['bufferData']),
                'bufferDistance': str(job['bufferDistance']),
                'reproject': str(job['reproject']),
                'reprojectEPSG': str(job['reprojectEPSG']),
                'keepIntermediates': str(job['keepIntermediates'])
            }))

            sbatchScriptPath = Path(job['workingFolder']).joinpath(f'{job['productName']}.sbatch'),

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
