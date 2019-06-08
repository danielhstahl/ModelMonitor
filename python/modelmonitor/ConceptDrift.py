from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from typing import List, Tuple
from pyspark import SparkContext
from pyspark.sql.utils import toJArray


def saveDistribution(dataframe, columnNameAndTypeArray:List[Tuple[str, str]], path:str)->bool:
    cdr = SimpleConceptDrift()
    return cdr.saveDistribution(dataframe, columnNameAndTypeArray, path)

def getNewDistributionsAndCompare(newDataSet,columnNameAndTypeArray:List[Tuple[str, str]], path:str)->dict:
    cdr = SimpleConceptDrift()
    return cdr.getNewDistributionsAndCompare(newDataSet, columnNameAndTypeArray, path)

class SimpleConceptDrift(object):
    def __init__(self):
        super(SimpleConceptDrift, self).__init__()
        self.ConceptDrift = _jvm().ml.dhs.modelmonitor.ConceptDrift
        self.gateway = SparkContext._gateway
        self.ColumnDescription = _jvm().ml.dhs.modelmonitor.ColumnDescription

    def saveDistribution(self, dataframe, columnNameAndTypeArray, path)->bool:
        _jColNameTypeArray=toJArray(
            self.gateway, 
            self.ColumnDescription, 
            [self.ColumnDescription(v[0], v[1]) for v in columnNameAndTypeArray]
        )
        return self.ConceptDrift.saveDistribution(
            self.ConceptDrift.getDistributions(dataframe._jdf, _jColNameTypeArray), 
            path
        )

    def getNewDistributionsAndCompare(self, newDataframe, columnNameAndTypeArray, path)->dict:
        results=self.ConceptDrift.getNewDistributionsAndCompare(
            newDataframe._jdf, 
            self.ConceptDrift.loadDistribution(path)
        )
        return {v[0]:results.get(v[0]).x() for v in columnNameAndTypeArray}
