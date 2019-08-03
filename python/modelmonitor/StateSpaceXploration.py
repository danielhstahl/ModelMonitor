from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from pyspark.sql import DataFrame
from typing import List, Tuple, Any
from pyspark.sql.utils import toJArray
from pyspark import SparkContext


class StateSpaceXploration(object):
    def __init__(self, seed: int):
        super(StateSpaceXploration, self).__init__()
        self._java_obj = _jvm().ml.dhs.modelmonitor.StateSpaceXploration(seed)
        self.ColumnSummary = _jvm().ml.dhs.modelmonitor.ColumnSummary
        self.ColumnType = _jvm().ml.dhs.modelmonitor.ColumnType
        self.gateway = SparkContext._gateway

    def generateDataSet(
        self, sparkSession, numSims: int, columns: List[Tuple[str, str, List[Any]]]
    ):
        _jColNameTypeArray = toJArray(
            self.gateway,
            self.ColumnSummary,
            [
                self.ColumnSummary(
                    v[0],
                    v[1],
                    self.gateway.jvm.scala.util.Left(
                        toJArray(self.gateway, self.gateway.jvm.double, v[2])
                    )
                    if v[1] == self.ColumnType.Numeric().toString()
                    else self.gateway.jvm.scala.util.Right(
                        toJArray(self.gateway, self.gateway.jvm.String, v[2])
                    ),
                )
                for v in columns
            ],
        )
        return DataFrame(
            self._java_obj.generateDataSet(
                sparkSession._instantiatedSession._jsparkSession,
                numSims,
                _jColNameTypeArray,
            ),
            sparkSession._instantiatedSession,
        )

    def getPredictions(
        self,
        sparkSession,
        simulatedDataSet,
        fittedPipeline,
        featuresCol="features",
        predictionCol="prediction",
    ):
        return DataFrame(
            self._java_obj.getPredictions(
                simulatedDataSet._jdf,
                fittedPipeline._to_java(),
                featuresCol,
                predictionCol,
            ),
            sparkSession._instantiatedSession,
        )

