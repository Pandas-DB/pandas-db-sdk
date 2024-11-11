# setup.py
from setuptools import setup, find_packages

setup(
    name="dataframe-storage-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "requests>=2.25.0",
        "boto3>=1.26.0"
    ],
)
