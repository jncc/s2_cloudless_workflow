from cloudmask.PrepareInputs import PrepareInputs
from cloudmask.cloudmasking.cloudmask import generateCloudMask
from luigi import LocalTarget
from luigi.util import requires
from s2cloudless import S2PixelCloudDetector

import json
import logging
import luigi
import numpy
import os

log = logging.getLogger('luigi-interface')

@requires(PrepareInputs)
class GenerateCloudmask(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDir = luigi.Parameter()

    cloudDetectorThreshold = luigi.FloatParameter(default=0.4)
    cloudDetectorAverageOver = luigi.IntParameter(default=4)
    cloudDetectorDilationSize = luigi.IntParameter(default=2)
    cloudDetectorAllBands = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        stackedTOARef = input['intermediateFiles']['stackedTOARef']

        cloudmask = generateCloudMask(self.safeDir, stackedTOARef, self.tempFolder, self.outputFolder, log, threshold=self.cloudDetectorThreshold, average_over=self.cloudDetectorAverageOver, dilation_size=self.cloudDetectorDilationSize, all_bands=self.cloudDetectorAllBands)

        output = {
            'intermediateFiles': {
                'anglesFile': input['intermediateFiles']['anglesFile'],
                'stackedTOA': input['intermediateFiles']['stackedTOA'],
                'stackedTOARef': input['intermediateFiles']['stackedTOARef'],
            },
            'cloud': {
                'mask': cloudmask,
                'probability': ''
            }
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        prepareInputsInfile = os.path.join(self.stateFolder, 'PrepareInputs.json')
        return LocalTarget(prepareInputsInfile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)