#!/bin/bash

# Unicode cheat sheet
# ✓ CTRL + SHIFT + U 2713
# ✗ CTRL + SHIFT + U 2717
# ⚠ CTRL + SHIFT + U 26A0

# Defaults to Airflow version 3.0.1
AIRFLOW_VERSION="3.0.1"

# Parse command line arguments
while getopts "v:h" opt; do
    case $opt in
        v)
            AIRFLOW_VERSION="$OPTARG"
            echo "Using Airflow version: $AIRFLOW_VERSION"
            ;;
        h)
            echo "Usage: $0 [-v version]"
            echo "  -v: Specify Airflow version (default: 3.0.1)"
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            echo "Use -h for help"
            exit 1
            ;;
    esac
done

echo "=== Apache Airflow Installation ==="
echo "Installing Airflow version: $AIRFLOW_VERSION"
echo "Installation directory: $(pwd)/airflow"
echo ""

# Check for uv package manager
echo "Checking for uv package manager..."
if uv --version &> /dev/null; then
    echo "✓ uv found: $(uv --version)"
else
    echo "✗ uv not found, installing..."
    pip install uv
    if uv --version &> /dev/null; then
        echo "✓ uv installed: $(uv --version)"
    else
        echo "✗ Failed to install uv"
        exit 1
    fi
fi

# Set up Airflow environment
export AIRFLOW_HOME
AIRFLOW_HOME="$(pwd)/airflow"
echo ""
echo "Setting AIRFLOW_HOME to: $AIRFLOW_HOME"

export AIRFLOW__CORE__LOAD_EXAMPLES
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Get Python version for constraints
echo ""
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
echo "Detected Python version: $PYTHON_VERSION"

# Validate Python version (Airflow supports 3.9-3.12)
case $PYTHON_VERSION in
    3.9|3.10|3.11|3.12)
        echo "✓ Python version is supported by Airflow"
        ;;
    *)
        echo "⚠ Warning: Python $PYTHON_VERSION may not be fully supported by Airflow"
        echo "  Supported versions: 3.9, 3.10, 3.11, 3.12"
        read -p "Continue anyway? (y/N): " -n 1 -r # return after 1 character and do not allow backslashes
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
        ;;
esac

# Build constraint URL
echo""
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
echo "Using constraints: $CONSTRAINT_URL"

# Export PYTHONPATH to include your project directory
export PYTHONPATH="/home/fastnnefarious/Projects/PycharmProjects/nasa_neo_pipeline:$PYTHONPATH"

# Install Airflow
echo ""
echo "Installing Apache Airflow..."
if uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"; then
    echo "✓ Airflow installation completed"
else
    echo "✗ Airflow installation failed"
    exit 1
fi

# Install Papermill Airflow provider
echo ""
echo "Installing Papermill Airflow provider..."
if uv pip install apache-airflow-providers-papermill ; then
    echo "✓ Papermill Airflow provider installation completed"
else
    echo "✗ Papermill Airflow provider installation failed"
    exit 1
fi

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
echo "=== Installation Complete ==="
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