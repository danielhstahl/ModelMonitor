## modelmonitor Python package

To run tests (assuming already installed dependencies):

`PYSPARK_PYTHON=python3 spark-submit --packages ml.dhs:modelmonitor_2.11:$VERSION-SNAPSHOT setup.py test`

This also will need to have the spark package published as described [here](../README.md).

## Use

### Concept Drift

```python
from modelmonitor import ConceptDrift
columnNameAndTypeArray=[
    ('actioncode', 'Categorical'),
    ('origin', 'Categorical'),
    ('examplenumeric', 'Numeric'),
]
ConceptDrift.saveDistribution(train_dataset, columnNameAndTypeArray, "./test_py.json")
results=ConceptDrift.getNewDistributionsAndCompare(test_dataset, columnNameAndTypeArray, "./test_py.json")
```

### State Space Exploration

```python
from modelmonitor.StateSpaceXploration import StateSpaceXploration
columns=[
    ("v1", "Categorical", ["a", "b", "c"]),
    ("v2", "Numeric", [-5.0, 5.0]),
    ("v3", "Numeric", [-5.0, 5.0]),
    ("v4", "Numeric", [-5.0, 5.0]),
    ("v5", "Categorical", ["f", "g", "h", "i"]),
]
encodeV1=StringIndexer(inputCol="v1", outputCol="v1_idx")
encodeV5=StringIndexer(inputCol="v5", outputCol="v5_idx")
assembleV=VectorAssembler(
    inputCols=["v1_idx", "v2", "v3", "v4", "v5_idx"], 
    outputCol="features"
)
model=RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
p=Pipeline(stages=[encodeV1, encodeV5, assembleV, model]).fit(self.train_dataset)
ssxi=ssx.StateSpaceXploration(42)
simDataSet=ssxi.generateDataSet(spark, 100000, columns)
result=ssxi.getPredictions(spark, simDataSet, p)
```


### Binary Metrics

```python
from modelmonitor.BinaryMetrics import getConfusionMatrix, getConfusionMatrixByGroup
results = getConfusionMatrix(dataset)
assert results["TN"] == 5
assert results["TP"] == 3
assert results["FN"] == 5
assert results["FP"] == 8

results = getConfusionMatrixByGroup(dataset, "group")
assert results["val1"]["TN"] == 4
assert results["val1"]["TP"] == 1
assert results["val1"]["FN"] == 2
assert results["val1"]["FP"] == 3
assert results["val2"]["TN"] == 1
assert results["val2"]["TP"] == 2
assert results["val2"]["FN"] == 3
assert results["val2"]["FP"] == 5

```