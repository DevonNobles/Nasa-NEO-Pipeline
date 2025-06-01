#!/bin/bash

# Set up Airflow environment
export AIRFLOW_HOME
AIRFLOW_HOME="$(pwd)/airflow"
echo ""
echo "Setting AIRFLOW_HOME to: $AIRFLOW_HOME"

export AIRFLOW__CORE__LOAD_EXAMPLES
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Initialize Airflow database
echo ""
echo "Installing Airflow database..."
if airflow standalone & then
    mkdir -p "$AIRFLOW_HOME" && echo "AIRFLOW_PID=$!" > "$AIRFLOW_HOME/service_pid.txt"
    echo "✓ Airflow standalone installed"
else
    echo "✗ Airflow standalone failed to install"
    exit 1
fi

# Creating standard Airflow directories
echo ""
echo "Checking for standard airflow directories (dags, logs, plugins, config)"
for dir in dags logs plugins config; do
    if [ ! -d "$AIRFLOW_HOME/$dir" ]; then
        mkdir -p "$AIRFLOW_HOME/$dir"
        echo "✓ $AIRFLOW_HOME/$dir directory created"
    else
        echo "✓ $AIRFLOW_HOME/$dir directory already exists"
    fi
done

echo ""
# Function to create management scripts
create_stop_script() {
    # Create stop script
    cat > "$AIRFLOW_HOME/stop_airflow.sh" << 'EOF'
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
EOF

    # Make script executable
    chmod +x "$AIRFLOW_HOME/stop_airflow.sh"
    echo "Stop Airflow script created: $AIRFLOW_HOME/stop_airflow.sh"
}

# Create management scripts
create_stop_script

echo ""
echo "=== Launching Airflow ==="
echo "Airflow has been installed in: $AIRFLOW_HOME"
echo ""
echo "Access the web UI at: http://localhost:8080"
echo "Username: admin"
echo "Password: Check the output above for the generated password"
echo ""
echo "Stop Airflow script created: $AIRFLOW_HOME/stop_airflow.sh"
echo ""
echo "Initializing all services!"
echo""