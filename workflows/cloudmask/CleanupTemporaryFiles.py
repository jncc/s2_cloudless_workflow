import json
import logging
import luigi
import os
import shutil

from cloudmask.MoveOutputFilesToFinalPath import MoveOutputFilesToFinalPath

from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(MoveOutputFilesToFinalPath)
class CleanupTemporaryFiles(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()

    keepIntermediates = luigi.BoolParameter(default=False)
    keepInputFiles = luigi.BoolParameter(default=False)
    deleteTempFolder = luigi.BoolParameter(default=False)

    def run(self):
        
        with self.input().open('r') as i:
            input = json.load(i)

        if not self.keepInputFiles:
            if input['inputs']['inputPathIsLink']:
                Path(input['inputs']['inputPath']).unlink()

                if input['inputs']['inputPath'] != input['inputs']['safeDir']:
                    # Safe Dir has been extracted into temp
                    shutil.rmtree(input['inputs']['safeDir'])
            else:
                if Path(input['inputs']['inputPath']).is_dir():
                    shutil.rmtree(input['inputs']['inputPath'])
                else:
                    Path(input['inputs']['inputPath']).unlink()
                    shutil.rmtree(input['inputs']['safeDir'])
      
        if not self.keepIntermediates:
            for path in os.listdir(self.tempFolder):
                if Path(self.tempFolder).joinpath(path).resolve().is_dir():
                    shutil.rmtree(Path(self.tempFolder).joinpath(path))
                else:
                    Path(self.tempFolder).joinpath(path).unlink()
            if self.deleteTempFolder:
                shutil.rmtree(self.tempFolder)

        output = input

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'MoveOutputFilesToFinalPath.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
