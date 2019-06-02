## modelmonitor Python package

To run tests (assuming already installed dependencies):

`PYSPARK_PYTHON=python3 PYTEST_ADDOPTS="--ignore=lib64" spark-submit setup.py test`