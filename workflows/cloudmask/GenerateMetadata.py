import json
import logging
import luigi
import os

from cloudmask.GenerateCloudShadowMask import GenerateCloudShadowMask

from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(GenerateCloudShadowMask)
class GenerateMetadata(luigi.Task):
    stateFolder = luigi.Parameter()
    basketFolder = luigi.Parameter()
    dataFolder = luigi.Parameter()

    def run(self):
        output = {
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)