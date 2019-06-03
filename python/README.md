## modelmonitor Python package

To run tests (assuming already installed dependencies):

`PYSPARK_PYTHON=python3 spark-submit --packages ml.dhs:modelmonitor_2.11:0.0.1 setup.py test`

This also will need to have the spark package published as described [here](../README.md).

## Use

```python
import modelmonitor.ConceptDrift as cdf
columnNameAndTypeArray=[
    ('actioncode', 'Categorical'),
    ('origin', 'Categorical'),
    ('examplenumeric', 'Numeric'),
]
cdf.saveDistribution(train_dataset, columnNameAndTypeArray, "./test_py.json")
results=cdf.getNewDistributionsAndCompare(test_dataset, columnNameAndTypeArray, "./test_py.json")
```