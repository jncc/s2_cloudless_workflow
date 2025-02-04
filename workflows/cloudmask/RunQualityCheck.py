import json
import logging
import luigi
import numpy as np
import os

from cloudmask.GenerateMetadata import GenerateMetadata

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal

log = logging.getLogger('luigi-interface')

@requires(GenerateMetadata)
class RunQualityCheck(luigi.Task):
    stateFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        output = input
        validValues = {}

        validValues.update(self.outputValidChecks('combinedCloudAndShadowMask', input))
        if not validValues['combinedCloudAndShadowMask']['valid']:
            raise RuntimeError(f'Found unexpected values in output image (combinedCloudAndShadowMask => {input['intermediateFiles']['combinedCloudAndShadowMask']}) => Found {validValues['combinedCloudAndShadowMask']['values']} : Expected {validValues['combinedCloudAndShadowMask']['expectedValues']}')

        if 'reprojectedCombinedCloudAndShadowMask' in input['intermediateFiles']:
            validValues.update(self.outputValidChecks('reprojectedCombinedCloudAndShadowMask', input))
            if not validValues['reprojectedCombinedCloudAndShadowMask']['valid']:
                raise RuntimeError(f'Found unexpected values in output image (reprojectedCombinedCloudAndShadowMask => {input['intermediateFiles']['reprojectedCombinedCloudAndShadowMask']}) => Found {validValues['reprojectedCombinedCloudAndShadowMask']['values']} : Expected {validValues['reprojectedCombinedCloudAndShadowMask']['expectedValues']}')

        output['qualityCheck'] = {
            'validValues': validValues
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def checkForValidPixels(self, fileToCheck, validValues = [1,2], nodataValue = 0):
        with gdal.Open(fileToCheck, gdal.GA_ReadOnly) as ds:
            values = np.unique(np.array(ds.GetRasterBand(1).ReadAsArray()))

        # No Data may not exist in the output, if it doesn't tack it on here so that the output is as expected
        if not (nodataValue in values):
            values = np.append([nodataValue], values)

        # Add no data value to the valid values list so the equivalence test works with the potentially non existant 
        # no data values in the output image
        validValues.append(nodataValue)

        return (np.array_equiv(sorted(values), validValues), values.tolist(), sorted(validValues))
    
    def outputValidChecks(self, type, statefile):
        (valid, values, expectedValues) = self.checkForValidPixels(statefile['intermediateFiles'][type])

        return {
            type: {   
                'valid': valid,
                'values': values,
                'expectedValues': expectedValues
            }
        }

    def input(self):
        infile = os.path.join(self.stateFolder, 'GenerateMetadata.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
