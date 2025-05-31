#!/bin/bash

echo "Current Airflow processes:"
pgrep -f airflow
read -p "Kill these processes? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  if [[ -f "$(dirname "$0")/service_pid.txt" ]]; then
      echo "Stopping Airflow services..."
      source "$(dirname "$0")/service_pid.txt"

      if [[ -n $AIRFLOW_PID ]] && kill -0 "$AIRFLOW_PID" 2> /dev/null; then # if AIRFLOW_PID not empty and PID exists
          kill "$AIRFLOW_PID"
          echo "PID: $AIRFLOW_PID stopped"
      else
          echo "PID not found"
      fi

      rm -f "$(dirname "$0")/service_pid.txt"
  fi

  echo
  echo "Killing Airflow processes gracefully (SIGTERM)..."
    pkill -f airflow

    # Wait a few seconds
    sleep 3

  REMAINING=$(pgrep -f airflow)
    if [ -z "$REMAINING" ]; then
        echo "All Airflow processes killed successfully."
    else
        echo "Some processes still running!"
        echo "Try pkill -9 -f airflow to force killing (SIGKILL)"
    fi
fi
