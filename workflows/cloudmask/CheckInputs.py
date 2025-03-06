import json
import logging
import luigi
import os
import pathlib
import zipfile

from luigi import LocalTarget

log = logging.getLogger('luigi-interface')

class CheckInputs(luigi.Task):
    stateFolder = luigi.Parameter()
    workingFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    inputPath = luigi.Parameter()

    def run(self):
        output = {}

        absInputPath = os.path.abspath(self.inputPath)

        output['inputs'] = {
            'inputPath': absInputPath
        }

        log.info(f'Supplied inputPath is {absInputPath}')


        if os.path.islink(absInputPath):
            log.info(f'inputPath is a symlink')
            output['inputs']['inputPathIsLink'] = True
        else:
            output['inputs']['inputPathIsLink'] = False

        if os.path.isdir(absInputPath):
            if (os.path.splitext(absInputPath)[1].lower() == '.safe'):
                log.info(f'inputPath appears to be a SAFE directory')
                output['inputs']['safeDir'] = absInputPath
            else:
                raise RuntimeError(f'Expected inputPath to be a .SAFE folder, inputPath is currently: {absInputPath}')
        elif (os.path.isfile(absInputPath)):
            if (os.path.splitext(absInputPath)[1].lower() == '.zip'):
                with zipfile.ZipFile(absInputPath, 'r') as inputZip:
                    zipContentsInitialPathParts = set(map(lambda path : pathlib.Path(path).parts[0], inputZip.namelist()))
                    
                    if len(zipContentsInitialPathParts) == 1:
                        safeDir = zipContentsInitialPathParts.pop()
                        
                        if os.path.splitext(safeDir)[1].lower() == '.safe':
                            log.info(f'inputPath is a zip file and contents will be extracted to {os.path.join(self.workingFolder, safeDir)}')
                            inputZip.extractall(self.workingFolder)

                            output['inputs'] = output['inputs'] | {
                                'zipFile': absInputPath,
                                'safeDir': os.path.join(self.workingFolder, safeDir)
                            }
                        else:
                            raise RuntimeError(f'Expected input Zip file to container a single .SAFE folder, found {safeDir}')
                    else:
                        raise RuntimeError(f'Expected input Zip file to container a single .SAFE folder, found {zipContentsInitialPathParts}')
            else:
                raise RuntimeError(f'Expected inputPath to be a .zip file, inputPath is currently: {absInputPath}')
        else:
            raise RuntimeError(f'Something unexpected happened with the input file, did not match any expected input')

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
