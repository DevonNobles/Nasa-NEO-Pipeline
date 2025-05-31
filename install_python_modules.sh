#!/bin/bash
set -e

# Install MinIO python module
if pip list | grep minio > /dev/null;then
    echo "minio in pip list"
else
    pip install minio
    echo "minio installed with pip"
fi

# Install Pyspark python module
if pip list | grep pyspark > /dev/null;then
    echo "pyspark in pip list"
else
    pip install pyspark==3.5.1
    echo "pyspark installed with pip"
fi

# Install redis python module
if pip list | grep redis > /dev/null;then
    echo "redis in pip list"
else
    pip install redis
    echo "redis installed with pip"
fi

# Install pyiceberg
pip install --upgrade pip
if [ ! $(pyiceberg --help &> /dev/null ; echo $?) = "0" ];then
  echo "Installing pyiceberg with pip"
  pip install pyiceberg["s3fs"]
else
  echo "pyiceberg already installed"
fi

# Install Jupyter Notebook python module
if pip list | grep notebook > /dev/null;then
    echo "Jupyter Notebook in pip list"
else
    pip install notebook
    echo "Jupyter Notebook installed with pip"
fi

# Install Regex python module
if pip list | grep regex > /dev/null;then
    echo "Regex in pip list"
else
    pip install regex
    echo "Regex installed with pip"
fi