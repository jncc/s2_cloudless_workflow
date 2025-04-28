import luigi
import json
import os
import logging
from datetime import datetime

from ceda_ard_finder import SearchForProducts, SearchTextFileList
from processCedaArchive.GetProductsFromGapReport import GetProductsFromGapReport
from processCedaArchive.GetProductsFromInputFolder import GetProductsFromInputFolder

log = logging.getLogger('luigi-interface')


class GetInputProducts(luigi.Task):
    stateFolder = luigi.Parameter()
    inputFolder = luigi.Parameter()

    def run(self):

        result = yield self.task()

        with result.open("r") as inputProducts:
            input = json.load(inputProducts)
            if len(input["productList"]) == 0:
                raise ValueError("No Products Found")

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(input, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "GetInputProducts.json"))


class GetRawProductsFromGapReport(GetInputProducts):
    gapReportRootDir = luigi.Parameter()
    gapReportPath = luigi.Parameter()
    gapReportMode = luigi.ChoiceParameter(default='useMatched', choices=['useMatched', 'useUnmatched', 'useTapeMatched'], var_type = str)

    def task(self):
        return GetProductsFromGapReport(
            stateFolder = self.stateFolder,
            gapReportmode = self.gapReportMode,
            gapReportPath = self.gapReportPath,
            gapReportRootPath = self.gapReportRootDir
        )

class GetRawProductsFromTextFileList(GetInputProducts):

    def task(self):
        return SearchTextFileList(
            stateFolder=self.stateFolder,
            productLocation=self.inputFolder
        )

class GetRawProductsFromFilters(GetInputProducts):
    # Ceda Ard Finder Params
    startDate = luigi.Parameter()  # Date in YYYY-MM-DD format
    endDate = luigi.Parameter()
    ardFilter = luigi.Parameter(default="")
    spatialOperator = luigi.ChoiceParameter(choices=["", "intersects", "disjoint", "contains", "within"])
    satelliteFilter = luigi.Parameter()

    # Optional, rarely used Params
    orbit = luigi.IntParameter(default=-9999)
    orbitDirection = luigi.Parameter(default="")
    wkt = luigi.Parameter(default="")

    def task(self):
        return SearchForProducts(
            stateFolder=self.stateFolder,
            startDate=datetime.strptime(self.startDate, "%Y-%m-%d"),  # Format str to datetime object
            endDate=datetime.strptime(self.endDate, "%Y-%m-%d"),
            ardFilter=self.ardFilter,
            spatialOperator=self.spatialOperator,
            satelliteFilter=self.satelliteFilter,
            orbit=self.orbit,
            orbitDirection=self.orbitDirection,
            wkt=self.wkt,
            elasticsearchPageSize=10000
        )
    
class GetRawProductsFromInputFolder(GetInputProducts):

    def task(self):
        return GetProductsFromInputFolder(
            stateFolder=self.stateFolder,
            inputFolder=self.inputFolder
        )