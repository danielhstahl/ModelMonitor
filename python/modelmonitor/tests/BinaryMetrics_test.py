from pyspark.sql import SparkSession, SQLContext
import modelmonitor.BinaryMetrics as bmf
import pandas as pd


def setup_module():
    global spark
    spark = SparkSession.builder.appName("LocalSpark").master("local").getOrCreate()


def teardown_module():
    spark.stop()


def create_dataset(spark):
    sqlCtx = SQLContext(spark.sparkContext)
    dataset = pd.DataFrame(
        data={
            "group": [
                "val1",
                "val1",
                "val1",
                "val1",
                "val1",
                "val1",
                "val1",
                "val1",
                "val1",
                "val1",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
                "val2",
            ],
            "label": [
                1.0,
                1.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
            "prediction": [
                0.0,
                0.0,
                1.0,
                1.0,
                1.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                0.0,
            ],
        }
    )
    return sqlCtx.createDataFrame(dataset)


class BinaryMetricsTest:
    @classmethod
    def setup_class(self):
        self.dataset = create_dataset(spark)

    def test_confusion_matrix(self):
        results = bmf.getConfusionMatrix(self.dataset)
        assert results["TN"] == 5
        assert results["TP"] == 3
        assert results["FN"] == 5
        assert results["FP"] == 8

    def test_confusion_matrix_by_group(self):
        results = bmf.getConfusionMatrixByGroup(self.dataset, "group")
        assert results["val1"]["TN"] == 4
        assert results["val1"]["TP"] == 1
        assert results["val1"]["FN"] == 2
        assert results["val1"]["FP"] == 3
        assert results["val2"]["TN"] == 1
        assert results["val2"]["TP"] == 2
        assert results["val2"]["FN"] == 3
        assert results["val2"]["FP"] == 5

