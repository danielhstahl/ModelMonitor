from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from typing import List, Tuple
from pyspark import SparkContext

def getDistributions(dataset, columnNameAndTypeArray:List[Tuple[str, str]]):
    cdr = SimpleConceptDrift()
    cdr.getDistributions(dataset, columnNameAndTypeArray)

def saveDistribution(distribution, path):
    cdr = SimpleConceptDrift()
    return cdr.saveDistribution(distribution, path)

def loadDistribution( path):
    cdr = SimpleConceptDrift()
    return cdr.loadDistribution(path)

def getNewDistributionsAndCompare(newDataSet, savedResult):
    cdr = SimpleConceptDrift()
    return cdr.getNewDistributionsAndCompare(newDataSet, savedResult)

#setattr(Transformer, 'getDistributions', getDistributions)
#setattr(Transformer, 'deserializeFromBundle', staticmethod(deserializeFromBundle))

class SimpleConceptDrift(object):
    def __init__(self):
        super(SimpleConceptDrift, self).__init__()
        self.ConceptDrift = _jvm().ml.dhs.modelmonitor.ConceptDrift
        self.ColumnDescription = _jvm().ml.dhs.modelmonitor.ColumnDescription

    def getDistributions(self, dataset, columnNameAndTypeArray):
        #string_class = _jvm().String
        jvmArray = SparkContext._gateway.new_array(self.ColumnDescription, len(columnNameAndTypeArray))
        for i, item in enumerate(columnNameAndTypeArray):
            jvmArray[i]=self.ColumnDescription(item[0], item[1])
        self.ConceptDrift.getDistributions(dataset._jdf, columnNameAndTypeArray)

    def saveDistribution(self, distribution, path)->bool:
        return self.ConceptDrift.saveDistribution(distribution, path)

    def loadDistribution(self, path):
        return self.ConceptDrift.loadDistribution(path)

    def getNewDistributionsAndCompare(self, newDataSet, savedResult):
        return self.ConceptDrift.getNewDistributionsAndCompare(newDataSet, savedResult)
       # return JavaTransformer._from_java( self._java_obj.deserializeFromBundle(path))