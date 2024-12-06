import logging
import luigi
import json
import os

from processCedaArchive.GetInputProducts import GetRawProductsFromFilters, GetRawProductsFromTextFileList
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

class PrepareWorkingDirectories(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    inputFolder = luigi.Parameter()
    jobStateFolder = luigi.Parameter()
    dataMounts = luigi.Parameter(default='')

    bufferData = luigi.BoolParameter(default=True)
    bufferDistance = luigi.Parameter(default='100')
    reproject = luigi.BoolParameter(default=True)
    reprojectionEPSG = luigi.Parameter(default='27700')
    keepIntermediates = luigi.BoolParameter(default=False)

    # Ceda Ard Finder Params. Set them to None if not used
    startDate = luigi.OptionalParameter(default=None)
    endDate = luigi.OptionalParameter(default=None)
    ardFilter = luigi.OptionalParameter(default="")
    spatialOperator = luigi.OptionalParameter(default=None)
    satelliteFilter = luigi.OptionalParameter(default=None)

    useInputList = luigi.BoolParameter(default=False)

    orbit = luigi.IntParameter(default=-9999)
    orbitDirection = luigi.Parameter(default='')
    wkt = luigi.Parameter(default='')    

    def requires(self):
        if self.useInputList:
            if Path(self.inputFolder).joinpath('inputs.txt').exists():
                return self.clone(GetRawProductsFromTextFileList)
            else:
                raise ValueError(f'Product Input list {self.productInputList} does not exist')
        else:
            # First check that the parameters are set correctly
            for k in ['startDate', 'endDate', 'ardFilter', 'spatialOperator', 'satelliteFilter']:
                if self.param_kwargs.get(k) is None:
                    raise ValueError(f'Parameter {k} is not set')
            return self.clone(GetRawProductsFromFilters)

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

            workingFolder = Path(self.tempFolder).joinpath(productName)
            workingFolder.mkdir()

            inputPath = Path(self.inputFolder).joinpath(productPath.name)
            inputPath.symlink_to(productPath)

            stateFolder = Path(self.jobStateFolder).joinpath(productPath.with_suffix('').name)
            stateFolder.mkdir()

            output['toProcess'].append({
                'productName': productName,
                'inputFolder': self.inputFolder,
                'inputPath': str(inputPath),
                'outputFolder': self.outputFolder,
                'stateFolder': str(stateFolder),
                'workingFolder': str(workingFolder),
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