import json
import logging
import luigi
import os

from cloudmask.CheckInputs import CheckInputs
from cloudmask.operations.sen2 import generateStackedImageAndAnglesFile, generateTOAReflectanceDN
from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(CheckInputs)
class PrepareInputs(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDir = luigi.Parameter()

    pixelSize = luigi.IntParameter(default=20)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        (stackedTOA, anglesFile) = generateStackedImageAndAnglesFile(self.safeDir, self.tempFolder, self.pixelSize, log)
        stackedTOAReflectance = generateTOAReflectanceDN(stackedTOA, self.safeDir, self.tempFolder, log)

        output = {
            "intermediateFiles": {
                "anglesFile": anglesFile,
                "stackedTOA": stackedTOA,
                "stackedTOARef": stackedTOAReflectance
            }
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'CheckInputs.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile) 
