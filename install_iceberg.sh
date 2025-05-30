#!/bin/bash
set -e # fail early

# download all required JARs for Iceberg + S3A support

## Iceberg runtime
#curl "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar" \
#  --create-dirs -o "$PWD/notebooks/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar"
#
## Hadoop runtime
#curl "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" \
#  --create-dirs -o "$PWD/notebooks/jars/hadoop-aws-3.3.6.jar"
#
## AWS Java SDK
#curl "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.500/aws-java-sdk-bundle-1.12.500.jar" \
#  --create-dirs -o "$PWD/notebooks/jars/aws-java-sdk-bundle-1.12.500.jar"

# Install pyiceberg
pip install --upgrade pip
if [ ! $(pyiceberg --help &> /dev/null ; echo $?) = "0" ];then
  echo "Installing pyiceberg with pip"
  pip install pyiceberg["s3fs"]
else
  echo "pyiceberg already installed"
fi

