import json
import logging
import luigi
import os

from luigi import LocalTarget
from luigi.util import requires
from PrepareWorkingDirectories import PrepareWorkingDirectories

log = logging.getLogger('luigi-interface')

@requires(PrepareWorkingDirectories)
class SetupWorkingDirectories(luigi.Task):
    stateFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()

    def run(self):
        with input().open('r') as i:
            output = json.load(i)

        
        

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'PrepareWorkingDirectories.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
