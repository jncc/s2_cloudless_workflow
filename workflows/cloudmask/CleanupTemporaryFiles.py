import json
import logging
import luigi
import os
import pathlib
import shutil

from cloudmask.RunQualityCheck import RunQualityCheck

from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(RunQualityCheck)
class CleanupTemporaryFiles(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()

    keepIntermediates = luigi.BoolParameter(default=False)
    keepInputFiles = luigi.BoolParameter(default=False)

    def run(self):
        
        with self.input().open('r') as i:
            input = json.load(i)

        if not self.keepInputFiles:
            if input['inputs']['inputPathIsLink']:
                os.unlink(input['inputs']['inputPath'])

                if input['inputs']['inputPath'] != input['inputs']['safeDir']:
                    # Safe Dir has been extracted into temp
                    shutil.rmtree(input['inputs']['safeDir'])
            else:
                if pathlib.Path(input['inputs']['inputPath']).is_dir():
                    shutil.rmtree(input['inputs']['inputPath'])
                else:
                    os.unlink(input['inputs']['inputPath'])
                    shutil.rmtree(input['inputs']['safeDir'])
      
        if not self.keepIntermediates:
            for path in os.listdir(self.tempFolder):
                if os.path.isdir(os.path.abspath(os.path.join(self.tempFolder, path))):
                    shutil.rmtree(os.path.join(self.tempFolder, path))
                else:
                    os.unlink(os.path.join(self.tempFolder, path))

        output = input

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'RunQualityCheck.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
