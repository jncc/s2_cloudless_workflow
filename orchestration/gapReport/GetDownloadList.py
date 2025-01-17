import logging
import luigi
import json
import os

from gapReport.GetProductListFromCDSE import GetProductListFromCDSE
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(GetProductListFromCDSE)
class GetDownloadList(luigi.Task):
    stateLocation = luigi.Parameter()

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        products = []
        for product in input['productList']:
            products.append(product['productID'])

        products = sorted(products)

        with self.output().open('w') as o:
            for product in products:
                o.write(f"{product}\n")
    
    def input(self):
        infile = Path(self.stateLocation).joinpath('GetProductListFromCDSE.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateLocation, f'{type(self).__name__}.txt')
        return LocalTarget(outFile)