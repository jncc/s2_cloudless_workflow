import logging
import luigi
import json
import os
import re

from datetime import datetime
from cdse_downloader.SearchForProducts import SearchForProducts
from cdse_downloader.SearchForProductsFromList import SearchForProductsFromList
from gapReport.GetProductListFromCDSE import GetProductListFromCDSE
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(GetProductListFromCDSE)
class GroupReprocessedData(luigi.Task):
    stateLocation = luigi.Parameter()

    def getGroupName(self, productName):
        exp = r'(S2[AB]{1})_MSIL1C_([0-9]{8}T[0-9]{6})_[A-Z0-9]{5}_[A-Z0-9]{4}_(T[0-9A-Z]{5})_[0-9]{8}T[0-9]{6}'
        match = re.search(exp, productName)
        return (match.group(1), match.group(2), match.group(3))

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        
        groups = {
            'S2A': {},
            'S2B': {}
        }
        for product in input['productList']:
            (sensor, captureTime, grid) = self.getGroupName(product['productID'])

            if captureTime in groups[sensor]:
                if grid in groups[sensor][captureTime]:
                    groups[sensor][captureTime][grid].append(product['productID'])
                else:
                    groups[sensor][captureTime][grid] = [product['productID']]
            else:
                groups[sensor][captureTime] = {
                    grid: [product['productID']]
                }

        output = {
            'groups': groups
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)
    
    def input(self):
        infile = Path(self.stateLocation).joinpath('GetProductListFromCDSE.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateLocation, f'{type(self).__name__}.json')
        return LocalTarget(outFile)