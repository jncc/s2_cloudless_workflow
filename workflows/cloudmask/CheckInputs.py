import json
import logging
import luigi
import os

from luigi import LocalTarget

log = logging.getLogger('luigi-interface')

class CheckInputs(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDir = luigi.Parameter()

    def run(self):
        output = {
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
