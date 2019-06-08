from pyspark.sql import SparkSession, SQLContext, functions
import modelmonitor.StateSpaceXploration as ssx
import pandas as pd
import random
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

def setup_module():
    global spark
    spark=SparkSession.builder.appName("LocalSpark").master("local").getOrCreate()

def teardown_module():
    spark.stop()

def create_train_dataset(spark):
    sqlCtx=SQLContext(spark.sparkContext)
    ssxi=ssx.StateSpaceXploration(42)
    columns=[
        ("v1", "Categorical", ["a", "b", "c"]),
        ("v2", "Numeric", [-5.0, 5.0]),
        ("v3", "Numeric", [-5.0, 5.0]),
        ("v4", "Numeric", [-5.0, 5.0]),
        ("v5", "Categorical", ["f", "g", "h", "i"]),
    ]
    numSims=500
    df=ssxi.generateDataSet(spark, numSims, columns)
    random.seed(42)
    def convertToLabel(v1, v2, v3, v4, v5):
        dv1=0.0
        if v1=="a":
            dv1=0.0
        elif v1=="b":
            dv1=0.25
        else:
            dv1=0.5
        dv5=0.0
        if v5=="f":
            dv5=0.0
        elif v5=="g":
            dv5=0.2
        elif v5=="h":
            dv5=0.3
        else:
            dv5=0.6
        threshold=dv1+(15.0+v2+v3+v4)/50.0+dv5
        if random.random()>threshold:
            return 1.0
        else:
            return 0.0
    udfConvertToLabel=functions.udf(convertToLabel, FloatType())
    return df.withColumn(
        "label", 
        udfConvertToLabel(df["v1"], df["v2"], df["v3"], df["v4"], df["v5"])
    )    

class TestEndToEndIntegration:
    @classmethod
    def setup_class(self):
        self.train_dataset=create_train_dataset(spark)
    def test_end_to_end_integration(self):
        columns=[
            ("v1", "Categorical", ["a", "b", "c"]),
            ("v2", "Numeric", [-5.0, 5.0]),
            ("v3", "Numeric", [-5.0, 5.0]),
            ("v4", "Numeric", [-5.0, 5.0]),
            ("v5", "Categorical", ["f", "g", "h", "i"]),
        ]
        encodeV1=StringIndexer(inputCol="v1", outputCol="v1_idx")
        encodeV5=StringIndexer(inputCol="v5", outputCol="v5_idx")
        assembleV=VectorAssembler(inputCols=["v1_idx", "v2", "v3", "v4", "v5_idx"], outputCol="features")
        model=RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
        p=Pipeline(stages=[encodeV1, encodeV5, assembleV, model]).fit(self.train_dataset)
        ssxi=ssx.StateSpaceXploration(42)
        simDataSet=ssxi.generateDataSet(spark, 100000, columns)
        result=ssxi.getPredictions(spark, simDataSet, p)
        assert result.count()<=100
        