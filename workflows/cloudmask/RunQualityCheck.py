import json
import logging
import luigi
import os

from cloudmask.GenerateCloudmask import GenerateCloudmask
from cloudmask.GenerateCloudShadowMask import GenerateCloudShadowMask
from cloudmask.GenerateMetadata import GenerateMetadata

from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(GenerateCloudmask, GenerateCloudShadowMask, GenerateMetadata)
class RunQualityCheck(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDirectory = luigi.Parameter()

    def run(self):

        inputs = self.input()

        output = {
            "cloud_mask": "",
            "cloud_probability": ""
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        return Input()

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)


class Input:
    cloudmask = {}
    shadowmask = {}
    metadata = {}

    def __init__(self):
        with LocalTarget(os.path.join(self.stateFolder, f'{type(GenerateCloudmask).__name__}.json')).open('r') as gc, \
            LocalTarget(os.path.join(self.stateFolder, f'{type(GenerateCloudShadowMask).__name__}.json')).open('r') as gcs, \
            LocalTarget(os.path.join(self.stateFolder, f'{type(GenerateMetadata).__name__}.json')).open('r') as gm:
            self.cloudmask = json.load(gc)
            self.shadowmask = json.load(gcs)
            self.metadata = json.load(gm)
