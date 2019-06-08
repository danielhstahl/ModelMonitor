from pyspark.sql import SparkSession, SQLContext
import modelmonitor.ConceptDrift as cdf
import pandas as pd
def setup_module():
    global spark
    spark=SparkSession.builder.appName("LocalSpark").master("local").getOrCreate()

def teardown_module():
    spark.stop()

def create_test_dataset(spark):
    sqlCtx=SQLContext(spark.sparkContext)
    dataset=pd.DataFrame(data={
        'v1':[
            'text1',
            'text2',
            'text3',
            'text2',
            'text2',
            'text1'
        ],
        'v2':[
            'val1', 'val2', 'val1',
            'val1', 'val2', 'val1'
        ],
        'examplenumeric':[2.0, 2.4, 1.4, -1.2, 1.2, 5.4]
    })
    return sqlCtx.createDataFrame(dataset)

def create_train_dataset(spark):
    sqlCtx=SQLContext(spark.sparkContext)
    dataset=pd.DataFrame(data={
        'v1':[
            'text1',
            'text2',
            'text3',
            'text2',
            'text1',
            'text1',
            'text2',
            'text3',
            'text2',
            'text2',
            'text1'
        ],
        'v2':[
            'val1', 'val2', 'val1',
            'val1', 'val2',
            'val1', 'val2', 'val1',
            'val1', 'val2', 'val1',
        ],
        'examplenumeric':[2.0, 2.4, 1.4, -1.2, 1.2, 5.4, 2.4, 1.4, -1.2, 1.2, 5.4]
    })
    return sqlCtx.createDataFrame(dataset)

class TestEndToEndIntegration:
    @classmethod
    def setup_class(self):
        self.test_dataset=create_test_dataset(spark)
        self.train_dataset=create_train_dataset(spark)
    def test_end_to_end_integration(self):
        columnNameAndTypeArray=[
            ('v1', 'Categorical'),
            ('v2', 'Categorical'),
            ('examplenumeric', 'Numeric'),
        ]
        cdf.saveDistribution(self.train_dataset, columnNameAndTypeArray, "./test_py.json")
        results=cdf.getNewDistributionsAndCompare(self.test_dataset, columnNameAndTypeArray, "./test_py.json")
        assert results['examplenumeric']>0
        assert results['v1']>0
        assert results['v2']>0
        