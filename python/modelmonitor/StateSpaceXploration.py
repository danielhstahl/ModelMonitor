from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from pyspark.sql import SQLContext
from typing import List, Tuple, Any
from pyspark.sql.utils import toJArray
from pyspark import SparkContext

class StateSpaceXploration(object):
    def __init__(self, seed:int):
        super(StateSpaceXploration, self).__init__()
        self._java_obj = _jvm().ml.dhs.modelmonitor.StateSpaceXploration(seed)
        self.ColumnSummary = _jvm().ml.dhs.modelmonitor.ColumnSummary
        self.gateway = SparkContext._gateway

    def generateDataSet(self, sc, numSims:int, columns:List[Tuple[str, str, List[Any]]]):
        _jColNameTypeArray=toJArray(
            self.gateway, 
            self.ColumnSummary, 
            [self.ColumnSummary(v[0], v[1], 
                toJArray(self.gateway, self.gateway.double if v[1]=="numeric" else self.gateway.string, v[2])
            ) for v in columns]
        )
        return self._java_obj.generateDataSet(sc, numSims, columns)

    def getPredictions(self, simulatedDataSet, p, featuresCol="features", predictionCol="prediction"):
        return self._java_obj.getPredictions(simulatedDataSet._jdf, p, featuresCol, predictionCol)