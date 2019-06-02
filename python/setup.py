from setuptools import setup, find_packages
import os
theLibFolder=os.path.dirname(os.path.realpath(__file__))
requirementPath=theLibFolder+'/requirements.txt'
install_requires=[]
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires=f.read().splitlines()

setup(
    name="modelmonitor",
    version="0.0.1",
    packages=find_packages(),
    install_requires=install_requires,
    long_description=open("README.md").read(),
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)