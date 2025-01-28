import json
import logging
import luigi
import os
import shutil

from cloudmask.Defaults import VERSION
from cloudmask.RunQualityCheck import RunQualityCheck

from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path


log = logging.getLogger('luigi-interface')

@requires(RunQualityCheck)
class MoveOutputFilesToFinalPath(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()

    reproject = luigi.BoolParameter(default=False)
    reprojectionEPSG = luigi.Parameter(default='')

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        output = input
        output['outputs'] = {}

        # Get SAFE dir base name to create output stem and create subfolders
        basename = Path(input['inputs']['safeDir']).with_suffix('').name
        cloudmask = Path(input['intermediateFiles']['combinedCloudAndShadowMask'])

        # Create output folder directories if required (in the form of {base}/original_projection/{year}/{month}/{day})
        outputImagePath = Path(self.outputFolder).joinpath(VERSION, 'original_projection', basename[11:15], basename[15:17], basename[17:19])
        outputImagePath.mkdir(parents=True, exist_ok=True)
        
        logging.info(f'Copying temporary output combined cloud and shadow mask to {outputImagePath.joinpath(cloudmask.name)}')
        shutil.copy(cloudmask, f'{outputImagePath.joinpath(cloudmask.name)}')
        output['outputs']['combinedCloudAndShadowMask'] = f'{outputImagePath.joinpath(cloudmask.name)}'
        
        if self.reproject and self.reprojectionEPSG:
            cloudmask = Path(input['intermediateFiles']['reprojectedCombinedCloudAndShadowMask'])
            # Create output folder directories if required (in the form of {base}/reprojected/epsg_{code}/{year}/{month}/{day})
            outputImagePath = Path(self.outputFolder).joinpath(VERSION, 'reprojected', f'epsg_{self.reprojectionEPSG}', basename[11:15], basename[15:17], basename[17:19])
            outputImagePath.mkdir(parents=True, exist_ok=True)

            logging.info(f'Copying temporary reprojected output combined cloud and shadow mask to {outputImagePath.joinpath(cloudmask.name.replace('final_', ''))}')
            shutil.copy(cloudmask, f'{outputImagePath.joinpath(cloudmask.name.replace('final_', ''))}')
            output['outputs']['reprojectedCombinedCloudAndShadowMask']  = f'{outputImagePath.joinpath(cloudmask.name)}'

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'RunQualityCheck.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
