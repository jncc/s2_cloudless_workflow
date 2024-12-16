import glob
import logging
import luigi
import json
import os

from gapReport.GroupReprocessedData import GroupReprocessedData
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(GroupReprocessedData)
class CompareToOnDiskArchive(luigi.Task):
    stateLocation = luigi.Parameter()
    rootDataPath = luigi.Parameter()
    
    def getPotentialDataPath(self, productName:str):
        path = Path(self.rootDataPath)

        if productName.startswith('S2A'):
            path = path.joinpath('sentinel2a')
        elif productName.startswith('S2B'):
            path = path.joinpath('sentinel2b')
        else:
            raise ValueError('Product didnt start with S2A or S2B')
        
        path = path.joinpath('data').joinpath('L1C_MSI')

        # Year
        #S2B_MSIL1C_20201001T112119_N0500_R037_T29UQV_20230411T200832
        path = path.joinpath(productName[11:15])
        # Month
        path = path.joinpath(productName[15:17])
        # Day
        path = path.joinpath(productName[17:19])

        return path

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        
        output = {
            'stats': {},
            'matchedGroups': {},
            'fuzzyMatched': {},
            'unmatchedGroups': {}
        }

        totalGroups = 0

        for sensor in input['groups']:
            for captureDate in input['groups'][sensor]:
                for grid in input['groups'][sensor][captureDate]:
                    totalGroups = totalGroups + 1
                    sortedProducts = sorted(input['groups'][sensor][captureDate][grid])
                    
                    matched = []
                    unmatched = []

                    for product in sortedProducts:
                        path = self.getPotentialDataPath(product)

                        if path.joinpath(f'{product}.zip').exists():
                            matched.append(product)
                        else:
                            unmatched.append(product)
                    
                    if len(matched) >= 1:
                        output['matchedGroups'][f'{sensor}_{captureDate}_{grid}'] = {
                            'matched': matched,
                            'unmatched': unmatched
                        }
                    else:
                        ## Attempt to find the group on disk and return the most recently reprocessed version
                        potentialMatches = [match[:-4] for match in glob.glob(f'{sensor}_MSIL1C_{captureDate}_*_{grid}_*.zip', root_dir=self.getPotentialDataPath(f'{sensor}_{captureDate}'))]

                        if len(potentialMatches) >= 1:
                            output['fuzzyMatched'] = {
                                'potentialMatch': potentialMatches,
                                'unmatched': list(set(unmatched) - set(potentialMatches))
                            }


                        output['unmatchedGroups'][f'{sensor}_{captureDate}_{grid}'] = {
                            'unmatched': unmatched
                        }

        output['stats'] = {
            'totalGroups': totalGroups, 
            'unmatchedGroups': len(output['unmatchedGroups'].keys())
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = Path(self.stateLocation).joinpath('GroupReprocessedData.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateLocation, f'{type(self).__name__}.json')
        return LocalTarget(outFile)