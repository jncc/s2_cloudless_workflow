import logging
import luigi
import json
import os

from ceda_ard_finder import SearchForProducts
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(SearchForProducts)
class PrepareWorkingDirectories(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputPath = luigi.Parameter()
    inputPath = luigi.Parameter()


    bufferData = luigi.BoolParameter(default=True)
    keepIntermediates = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        
        output = input
        output['workingFolders'] = []

        for product in input['productList']:
            productPath = Path(product)
            productName = productPath.with_suffix('').name

            workingDir = Path.joinpath(self.tempFolder, productName)
            workingDir.mkdir()

            inputPath = workingDir.joinpath(productPath.name)
            inputPath.symlink_to(productPath)

            stateDir = workingDir.joinpath('state')
            stateDir.mkdir()

            tempDir = workingDir.joinpath('temp')
            tempDir.mkdir()

            output['toProcess'].append({
                'inputPath': inputPath,
                'outputPath': self.outputPath,
                'stateDir': stateDir,
                'tempDir': tempDir,
                'workingDir': workingDir
            })

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'SearchForProducts.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)