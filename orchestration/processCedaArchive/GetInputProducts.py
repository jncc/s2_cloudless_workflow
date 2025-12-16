import luigi
import json
import os
import logging
import glob

from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger('luigi-interface')


class GetInputProducts(luigi.Task):
    stateFolder = luigi.Parameter()
    inputFolder = luigi.Parameter()
    dataFolder = luigi.Parameter(default=None)

    def getProductsFromFolder(self):
        return glob.glob(os.path.join(self.inputFolder, "S2*"))
    
    def getProductsFromFile(self, inputFile):
        productList = set()

        with open(inputFile) as f:
            for line in f:
                if not line.strip():
                    continue
                elif line.startswith("S2"):
                    productList.add(line.rstrip())
                else:
                    log.info(f'Ignoring {line.rstrip}, name does not start with "S2"')
                    continue

        return list(productList)
    
    def getAllDates(self, startDate, endDate):
        allDates = []

        start = datetime.strptime(startDate, "%Y-%m-%d")
        end = datetime.strptime(endDate, "%Y-%m-%d")
        for x in range((end-start).days + 1):
            allDates.append(start+timedelta(days=x))

        return allDates
    
    def createSymlinks(self, products):
        if self.dataFolder:
            for product in products:
                if not product.endswith(".zip"):
                    productName = f"{product}.zip"
                else:
                    productName = product

                sourcePath = os.path.join(self.dataFolder, product[11:15], product[15:17], product[17:19], productName)
                destPath = os.path.join(self.inputFolder, productName)
                if not Path(sourcePath).exists:
                    raise Exception(f"{sourcePath} not found, can't create symlink")

                os.symlink(sourcePath, destPath)
        else:
            raise Exception("dataFolder param needs to be set to create symlinks")

    def run(self):
        productList = []

        inputFile = Path(os.path.join(self.inputFolder, "inputs.txt"))
        if inputFile.is_file():
            log.info("Using inputs file")
            productList = self.getProductsFromFile(inputFile)
            self.createSymlinks(productList)
            inputFile.unlink()
        else:
            productList = self.getProductsFromFolder()

        if len(productList) == 0:
            raise ValueError("No Products Found")

        output = {
            "productList": productList
        }
        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "GetInputProducts.json"))
    