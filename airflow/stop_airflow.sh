#!/bin/bash

if [[ -f "$(dirname $0)/service_pid.txt" ]]; then
    echo "Stopping Airflow services..."
    source "$(dirname $0)/service_pid.txt"

    if [[ -n $AIRFLOW_PID ]] && kill -0 "$AIRFLOW_PID" 2> /dev/null; then # if AIRFLOW_PID not empty and PID exists
        kill "$AIRFLOW_PID"
        echo "PID: $AIRFLOW_PID stopped"
    else
        echo "Airflow services not running or PID not found"
    fi

    # rm -f "$AIRFLOW_HOME/service_pid.txt"
else
    echo "No PID file found. Services may not be running."
fi
