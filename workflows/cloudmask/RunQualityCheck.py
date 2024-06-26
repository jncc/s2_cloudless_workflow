import json
import logging
import luigi
import os

from cloudmask.GenerateMetadata import GenerateMetadata

from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(GenerateMetadata)
class RunQualityCheck(luigi.Task):
    stateFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        output = input
        output['qualityCheck'] = {}

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'GenerateMetadata.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
