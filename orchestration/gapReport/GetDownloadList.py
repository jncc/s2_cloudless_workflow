import logging
import luigi
import json
import os

from gapReport.CompareToOnDiskArchive import CompareToOnDiskArchive
from luigi import LocalTarget
from luigi.util import requires
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(CompareToOnDiskArchive)
class GetDownloadList(luigi.Task):
    stateLocation = luigi.Parameter()

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        products = set()
        for unmatched in input['unmatchedGroups']:
            for product in input["unmatchedGroups"][unmatched]['unmatched']:
                products.add(product)

        products = sorted(products)

        with self.output().open('w') as o:
            for product in products:
                o.write(f"{product}\n")
    
    def input(self):
        infile = Path(self.stateLocation).joinpath('CompareToOnDiskArchive.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateLocation, f'{type(self).__name__}.txt')
        return LocalTarget(outFile)