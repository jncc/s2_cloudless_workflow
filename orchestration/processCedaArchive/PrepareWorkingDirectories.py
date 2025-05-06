import logging
import luigi
import json
import os

from processCedaArchive.GetInputProducts import GetInputProducts
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(GetInputProducts)
class PrepareWorkingDirectories(luigi.Task):
    stateFolder = luigi.Parameter()
    workingFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    inputFolder = luigi.Parameter()
    jobStateFolder = luigi.Parameter()
    dataMounts = luigi.Parameter(default='')

    bufferData = luigi.BoolParameter(default=True)
    bufferDistance = luigi.Parameter(default='100')
    reproject = luigi.BoolParameter(default=True)
    reprojectionEPSG = luigi.Parameter(default='27700')
    keepIntermediates = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        
        output = {
            'toProcess': []
        }

        for product in input['productList']:
            log.info(f'Creating processing directories for {product}')

            productPath = Path(product)
            productName = productPath.with_suffix('').name

            workingFolder = Path(self.workingFolder).joinpath(productName)
            workingFolder.mkdir(exist_ok=True)

            tmpFolder = Path(workingFolder).joinpath('tmp')
            tmpFolder.mkdir(exist_ok=True)

            inputPath = Path(self.inputFolder).joinpath(productPath.name)

            if not inputPath.exists:
                inputPath.symlink_to(productPath)

            stateFolder = Path(self.jobStateFolder).joinpath(productPath.with_suffix('').name)
            stateFolder.mkdir(exist_ok=True)

            output['toProcess'].append({
                'productName': productName,
                'inputFolder': self.inputFolder,
                'inputPath': str(inputPath),
                'outputFolder': self.outputFolder,
                'stateFolder': str(stateFolder),
                'workingFolder': str(workingFolder),
                'tmpFolder': str(tmpFolder),
                'dataMounts': self.dataMounts,
                'bufferData': self.bufferData,
                'bufferDistance': self.bufferDistance,
                'reproject': self.reproject,
                'reprojectionEPSG': self.reprojectionEPSG,
                'keepIntermediates': self.keepIntermediates
            })

        log.info(f'Created processing directories for {len(output["toProcess"])} products.')

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'GetInputProducts.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)