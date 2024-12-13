import logging
import luigi
import json
import os

from datetime import datetime
from cdse_downloader.SearchForProducts import SearchForProducts
from cdse_downloader.SearchForProductsFromList import SearchForProductsFromList
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

class GetProductListFromCDSE(luigi.Task):
    stateLocation = luigi.Parameter()
    productListFile = luigi.Parameter(default='')
    mission = luigi.Parameter('sentinel2')
    maxrecords = luigi.IntParameter(default=100)
    #defaults are just to negate these parameters not meaningful search values
    startDate = luigi.DateMinuteParameter(default=datetime.now())
    endDate = luigi.DateMinuteParameter(default=datetime.now())
    wkt = luigi.Parameter(default='')
    s2CloudCover = luigi.IntParameter(default=100)
    onlineOnly = luigi.BoolParameter(default=True)
    productType = luigi.Parameter(default='S2MSI1C')

    params = {}

    def requires(self):
        if self.productListFile:
            if Path(self.productListFile).exists():
                return self.clone(SearchForProductsFromList)
            else:
                raise ValueError(f'Product Input list {self.productListFile} does not exist')
        else:
            return self.clone(SearchForProducts)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        
        output = input

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.stateLocation, f'{type(self).__name__}.json')
        return LocalTarget(outFile)