import luigi
import json
import os
import logging
import glob

from luigi import LocalTarget

log = logging.getLogger('luigi-interface')

class GetProductsFromInputFolder(luigi.Task):
    stateFolder = luigi.Parameter()
    inputFolder = luigi.Parameter()

    def run(self):
        products = glob.glob(self.inputFolder, "S2*")

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(products, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "GetProductsFromInputFolder.json"))