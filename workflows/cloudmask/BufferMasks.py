import json
import logging
import luigi
import os

from cloudmask.GenerateCloudShadowMask import GenerateCloudShadowMask
from cloudmask.operations.buffering import bufferData, polygonizeData, rasterizeData

from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(GenerateCloudShadowMask)
class BufferMasks(luigi.Task):
    stateFolder = luigi.Parameter()
    workingFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    inputPath = luigi.Parameter()

    bufferData = luigi.BoolParameter(default=True)
    bufferDistance = luigi.IntParameter(default=100)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        output = input

        if bufferData:
            log.info('Buffering cloud and cloud shadow masks')
            basename = Path(input['inputs']['safeDir']).stem
            output['intermediateFiles']['buffered'] = {}

            log.info(f'Polygonize cloud layer - {input['intermediateFiles']['cloudMask']}')
            polygonizedDataPath = polygonizeData(self.workingFolder, basename, 'cloud', input['intermediateFiles']['cloudMask'])
            log.info(f'Buffer Polygonized layer - {polygonizedDataPath}')
            (bufferedDataPath, bufferedDataLayerName, bufferedDataLayerField) = bufferData(polygonizedDataPath, self.workingFolder, 'cloud', 'cloud', fieldValue=1, outputFieldValue=1, bufferDist=100)
            output['intermediateFiles']['buffered']['cloud'] = rasterizeData(bufferedDataPath, bufferedDataLayerName, bufferedDataLayerField, input['intermediateFiles']['stackedTOA'], self.workingFolder, basename, 'cloud')
               
            log.info(f'Polygonize cloud shadow layer - {input['intermediateFiles']['cloudShadowMask']}')
            polygonizedDataPath = polygonizeData(self.workingFolder, basename, 'shadow', input['intermediateFiles']['cloudShadowMask'])
            log.info(f'Buffer Polygonized layer - {polygonizedDataPath}')
            (bufferedDataPath, bufferedDataLayerName, bufferedDataLayerField) = bufferData(polygonizedDataPath, self.workingFolder, 'shadow', 'shadow', fieldValue=1, outputFieldValue=2, bufferDist=100)
            output['intermediateFiles']['buffered']['shadow'] = rasterizeData(bufferedDataPath, bufferedDataLayerName, bufferedDataLayerField, input['intermediateFiles']['stackedTOA'], self.workingFolder, basename, 'shadow')

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'GenerateCloudShadowMask.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)