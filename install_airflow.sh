#!/bin/bash

# Defaults to Airflow version 3.0.1
AIRFLOW_VERSION="3.0.1"

while getopts "v:" opt; do
	 case $opt in
 		v)
 			AIRFLOW_VERSION="$OPTARG"
			echo "Using Airflow version: $AIRFLOW_VERSION"
			;;
 		\?)
 			echo "Invalid option: -$OPTARG"
 			exit 1
			;;
	esac
done

echo "Beginning Apache Airflow version $AIRFLOW_VERSION installation..."
echo  "Installing via uv"
echo "Checking for uv..."

if uv --version &> /dev/null; then
	uv --version
	echo "found"
else
	echo "uv not found"
	echo "Installing uv python package manager with pip"
	pip install uv
        ver=$(uv --version)
        echo "$ver installed"
fi

export AIRFLOW_HOME="$PWD"
echo $AIRFLOW_HOME

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.9: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.1/constraints-3.9.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
