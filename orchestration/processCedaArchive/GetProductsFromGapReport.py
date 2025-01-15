import logging
import luigi
import json
import os

from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

class GetProductsFromGapReport(luigi.Task):
    stateFolder = luigi.Parameter()

    gapReportmode = luigi.ChoiceParameter(default='useMatched', choices=['useMatched', 'useUnmatched', 'useTapeMatched'], var_type=str)
    gapReportPath = luigi.Parameter()
    gapReportRootPath = luigi.Parameter()

    def getInputPath(self, productName:str, rootPath:str, downloaded:bool = False):
        path = Path(rootPath)

        if not downloaded:
            if productName.startswith('S2A'):
                path = path.joinpath('sentinel2a')
            elif productName.startswith('S2B'):
                path = path.joinpath('sentinel2b')
            else:
                raise ValueError('Product didnt start with S2A or S2B')
            
            path = path.joinpath('data').joinpath('L1C_MSI')

            # Year
            path = path.joinpath(productName[11:15])
            # Month
            path = path.joinpath(productName[15:17])
            # Day
            path = path.joinpath(productName[17:19])

        return path
    
    def productNamesToInputFileList(self, groups, mode):
        productList = []

        for group in groups:
            for product in groups[group][mode]:
                downloaded = False

                if mode is 'unmatched':
                    downloaded = True

                productPath = Path(self.getInputPath(product, self.gapReportRootPath, downloaded)).joinpath(f'{product}.zip')
                if productPath.exists():
                    productList.append(f'{productPath}')
                else:
                    raise ValueError('Product {product} not found at expected path: {productPath}')
        
        return productList

    def run(self):
        with open(self.gapReportPath, 'r') as input:
            inputs = json.load(input)
        
        output = {
            'productList': []
        }

        if self.mode is 'useMatched':
            output['productList'] = self.productNamesToInputFileList(inputs['matchedGroups'], 'matched')
        elif self.mode is 'useUnmatched':
            output['productList'] = self.productNamesToInputFileList(inputs['unmatchedGroups'], 'unmatched')
        elif self.mode is 'useTapeMatched':
            output['productList'] = self.productNamesToInputFileList(inputs['matchedGroups'], 'matchedOnTape')



        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)