from setuptools import setup, find_packages


setup(
    name="modelmonitor",
    version="0.0.1",
    packages=find_packages(),
    long_description=open("README.md").read(),
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)