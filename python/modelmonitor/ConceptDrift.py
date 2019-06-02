from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from typing import List, Tuple

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
        self._java_obj = _jvm().ml.dhs.modelmonitor.ConceptDrift

    def getDistributions(self, dataset, columnNameAndTypeArray):
        self._java_obj.getDistributions(dataset._jdf, columnNameAndTypeArray)

    def saveDistribution(self, distribution, path)->bool:
        return self._java_obj.saveDistribution(distribution, path)

    def loadDistribution(self, path):
        return self._java_obj.loadDistribution(path)

    def getNewDistributionsAndCompare(self, newDataSet, savedResult):
        return self._java_obj.getNewDistributionsAndCompare(newDataSet, savedResult)
       # return JavaTransformer._from_java( self._java_obj.deserializeFromBundle(path))