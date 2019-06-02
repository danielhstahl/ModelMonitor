from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from typing import List, Tuple
from pyspark import SparkContext
from pyspark.sql.utils import toJArray


def saveDistribution(dataframe, columnNameAndTypeArray:List[Tuple[str, str]], path:str):
    cdr = SimpleConceptDrift()
    return cdr.saveDistribution(dataframe, columnNameAndTypeArray, path)

def getNewDistributionsAndCompare(newDataSet, path:str):
    cdr = SimpleConceptDrift()
    return cdr.getNewDistributionsAndCompare(newDataSet, path)

#setattr(Transformer, 'getDistributions', getDistributions)
#setattr(Transformer, 'deserializeFromBundle', staticmethod(deserializeFromBundle))

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
        #for i, item in enumerate(columnNameAndTypeArray):
        #    _jColNameTypeArray[i]=self.ColumnDescription(item[0], item[1])
        #self.ConceptDrift.getDistributions(dataset._jdf, _jColNameTypeArray)
        return self.ConceptDrift.saveDistribution(
            self.ConceptDrift.getDistributions(dataframe._jdf, _jColNameTypeArray), 
            path
        )

    def getNewDistributionsAndCompare(self, newDataframe, path):
        return self.ConceptDrift.getNewDistributionsAndCompare(
            newDataframe._jdf, 
            self.ConceptDrift.loadDistribution(path)
        )
       # return JavaTransformer._from_java( self._java_obj.deserializeFromBundle(path))