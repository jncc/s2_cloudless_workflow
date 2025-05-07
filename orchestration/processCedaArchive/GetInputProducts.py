import luigi
import json
import os
import logging
import glob
import re

from datetime import datetime, timedelta
from pathlib import Path
from processCedaArchive.GetProductsFromGapReport import GetProductsFromGapReport
from processCedaArchive.GetProductsFromInputFolder import GetProductsFromInputFolder

log = logging.getLogger('luigi-interface')


class GetInputProducts(luigi.Task):
    stateFolder = luigi.Parameter()
    inputFolder = luigi.Parameter()

    startDate = luigi.Parameter(default="")  # Date in YYYY-MM-DD format
    endDate = luigi.Parameter(default="")
    ardFilter = luigi.Parameter(default="")
    dataFolder = luigi.Parameter()

    useInputList = luigi.BoolParameter(default=False)
    skipSearch = luigi.BoolParameter(default=False)

    def getProductsFromFolder(self):
        return glob.glob(os.path.join(self.inputFolder, "S2*"))
    
    def getProductsFromFile(self):
        productList = []
        inputList = Path(self.inputFolder).joinpath('inputs.txt')
        with open(inputList) as f:
            productList = json.load(f)

        return productList
    
    def getAllDates(self, startDate, endDate):
        allDates = []

        start = datetime.strptime(startDate, "%Y-%m-%d")
        end = datetime.strptime(endDate, "%Y-%m-%d")
        for x in range((end-start).days):
            allDates.append(start+timedelta(days=x))

        return allDates
    
    def getFilteredProducts(self, allProducts):        
        filteredProducts = []
        for product in allProducts:
            if self.ardFilter:
                matches = re.search(self.ardFilter, product)
                if matches:
                    filteredProducts.append(matches.group(0))
            else:
                filteredProducts.append(os.path.basename(product))

        return filteredProducts

    def searchForProducts(self):
        allDates = self.getAllDates(self.startDate, self.endDate)

        filteredProducts = []
        for date in allDates:
            dateString = date.isoformat() # YYYY-MM-DD
            datePath = os.path.join(self.dataFolder, dateString[:4], dateString[5:7], dateString[8:10])
            allProductsForDate = glob.glob(os.path.join(datePath, "S2*.zip"))

            filteredProducts.extend(self.getFilteredProducts(allProductsForDate))

        return filteredProducts
    
    def createSymlinks(self, products):
        for product in products:
            sourcePath = os.path.join(self.dataFolder, product[11:15], product[15:17], product[17:19], product)
            destPath = os.path.join(self.inputFolder, product)
            if not Path(sourcePath).exists:
                raise Exception(f"{sourcePath} not found, can't create symlink")

            os.symlink(sourcePath, destPath)

    def run(self):
        productList = []
        if self.skipSearch:
            productList = self.getProductsFromFolder()
        elif self.useInputList:
            productList = self.getProductsFromFile()
            self.createSymlinks(productList)
        else:
            productList = self.searchForProducts()
            self.createSymlinks(productList)

        if len(productList) == 0:
            raise ValueError("No Products Found")

        output = {
            "productList": productList
        }
        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "GetInputProducts.json"))
    