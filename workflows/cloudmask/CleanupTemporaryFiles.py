import json
import logging
import luigi
import os

from cloudmask.RunQualityCheck import RunQualityCheck

from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(RunQualityCheck)
class CleanupTemporaryFiles(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()

    keepIntermediates = luigi.BoolParameter(default=False)
    keepLooseFiles = luigi.BoolParameter(default=False)

    def run(self):
        
        with self.input().open('r') as i:
            input = json.load(i)

        if not self.keepIntermediates:
            for key in input['intermediateFiles']:
                os.unlink(input['intermediateFiles'][key])
        
        if not self.keepLooseFiles:
            for path in os.listdir(self.tempFolder):
                os.unlink(os.path.join(self.tempFolder, path))

        del input['intermediateFiles']
        output = input

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'RunQualityCheck.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
