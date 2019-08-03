from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm
from typing import List, Tuple
from pyspark import SparkContext
from pyspark.sql.utils import toJArray


def getConfusionMatrix(
    dataframe, labelCol: str = "label", predictionCol: str = "prediction"
) -> dict:
    bmr = SimpleBinaryMetrics()
    return bmr.getConfusionMatrix(dataframe, labelCol, predictionCol)


def getConfusionMatrixByGroup(
    dataframe, groupCol: str, labelCol: str = "label", predictionCol: str = "prediction"
) -> dict:
    bmr = SimpleBinaryMetrics()
    return bmr.getConfusionMatrixByGroup(dataframe, groupCol, labelCol, predictionCol)


def _convertBinaryConfusionMatrix(results) -> dict:
    return {
        "TN": results.TN(),
        "TP": results.TP(),
        "FN": results.FN(),
        "FP": results.FP(),
    }


class SimpleBinaryMetrics(object):
    def __init__(self):
        super(SimpleBinaryMetrics, self).__init__()
        self.BinaryMetrics = _jvm().ml.dhs.modelmonitor.BinaryMetrics

    def getConfusionMatrix(self, dataframe, labelCol: str, predictionCol: str) -> dict:
        results = self.BinaryMetrics.getConfusionMatrix(
            dataframe._jdf, labelCol, predictionCol
        )
        return _convertBinaryConfusionMatrix(results)

    def getConfusionMatrixByGroup(
        self, dataframe, groupCol: str, labelCol: str, predictionCol: str
    ) -> dict:
        results = self.BinaryMetrics._convertToJava(
            self.BinaryMetrics.getConfusionMatrixByGroup(
                dataframe._jdf, groupCol, labelCol, predictionCol
            )
        )
        return {key: _convertBinaryConfusionMatrix(results.get(key)) for key in results}
