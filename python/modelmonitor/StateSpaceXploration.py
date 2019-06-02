from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from pyspark.sql import SQLContext
from typing import List, Tuple

class StateSpaceXploration(object):
    def __init__(self, seed):
        super(StateSpaceXploration, self).__init__()
        self._java_obj = _jvm().ml.dhs.modelmonitor.StateSpaceXploration(seed)

    def generateDataSet(self, sc, numSims, columns):
        sqlContext = SQLContext(sc)
        return self._java_obj.generateDataSet(sc, sqlContext, numSims, columns)

    def getPredictions(self, simulatedDataSet, p):
        return self._java_obj.getPredictions(simulatedDataSet, p)