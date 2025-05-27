#!/bin/bash

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
echo "Setting AIRFLOW_HOME to: $AIRFLOW_HOME"

export AIRFLOW__CORE__LOAD_EXAMPLES
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Get Python version for constraints
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
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
echo "Using constraints: $CONSTRAINT_URL"

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
uv pip install apache-airflow-providers-papermill \

# Initialize Airflow database
echo ""
airflow standalone
#echo ""
#echo "Initializing Airflow database..."
#if airflow db migrate; then
#    echo "✓ Airflow database initialized"
#else
#    echo "✗ Failed to initialize Airflow database"
#    exit 1
#fi
#
## Create admin user
#echo ""
#echo "Creating admin user..."
#echo ""
#airflow users create \
#    --username admin \
#    --firstname Admin \
#    --lastname User \
#    --role Admin \
#    --use-random-password
#
#mkdir -p "${AIRFLOW_HOME}"/{dags,logs,plugins,config}
#
## Function to start services
#start_services() {
#    echo ""
#    echo "Starting Airflow services..."
#
#    # Start webserver
#    echo ""
#    echo "Starting webserver..."
#    nohup airflow api-server --port 8080 -Dd -A - -l "$AIRFLOW_HOME/logs/api-server"
#    WEBSERVER_PID=$!
#    echo "✓ Webserver started (PID: $WEBSERVER_PID)"
#
#    # Give webserver a moment to start
#    sleep 2
#
#    # Start scheduler
#    echo ""
#    echo "Starting scheduler..."
#    nohup airflow scheduler -D -l "$AIRFLOW_HOME/logs/scheduler"
#    SCHEDULER_PID=$!
#    echo "✓ Scheduler started (PID: $SCHEDULER_PID)"
#
#    # Start dag-processor
#    echo ""
#    echo "Starting dag-processor..."
#    export AIRFLOW__CORE__LOAD_EXAMPLES
#    AIRFLOW__CORE__LOAD_EXAMPLES=false
#    nohup airflow dag-processor -D -l "$AIRFLOW_HOME/logs/dag-processor"
#    DAG_PROCESSOR_PID=$!
#    echo "✓ DAG processor started (PID: $DAG_PROCESSOR_PID)"
#
#    # Start triggerer
#    echo ""
#    echo "Starting triggerer..."
#    nohup airflow triggerer -D -l "$AIRFLOW_HOME/logs/triggerer"
#    TRIGGERER_PID=$!
#    echo "✓ Triggerer started (PID: $TRIGGERER_PID)"
#
#    # Save PIDs to file for easy management
#    cat > "$AIRFLOW_HOME/service_pids.txt" << EOF
#WEBSERVER_PID=$WEBSERVER_PID
#SCHEDULER_PID=$SCHEDULER_PID
#DAG_PROCESSOR_PID=$DAG_PROCESSOR_PID
#TRIGGERER_PID=$TRIGGERER_PID
#EOF
#
#    echo ""
#    echo "All services started successfully!"
#    echo "Service PIDs saved to: $AIRFLOW_HOME/service_pids.txt"
#}
#
## Function to create management scripts
#create_management_scripts() {
#    # Create stop script
#    cat > "$AIRFLOW_HOME/stop_airflow.sh" << 'EOF'
##!/bin/bash
#AIRFLOW_HOME="$(dirname "$0")"
#PID_FILE="$AIRFLOW_HOME/service_pids.txt"
#
#if [[ -f "$PID_FILE" ]]; then
#    echo "Stopping Airflow services..."
#    source "$PID_FILE"
#
#    for service in WEBSERVER SCHEDULER DAG_PROCESSOR TRIGGERER; do
#        pid_var="${service}_PID"
#        pid=${!pid_var}
#        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
#            echo "Stopping $service (PID: $pid)..."
#            kill "$pid"
#        else
#            echo "$service is not running or PID not found"
#        fi
#    done
#
#    echo "Airflow services stopped."
#    rm -f "$PID_FILE"
#else
#    echo "No PID file found. Services may not be running."
#fi
#EOF
#
#    # Create status script
#    cat > "$AIRFLOW_HOME/status_airflow.sh" << 'EOF'
##!/bin/bash
#AIRFLOW_HOME="$(dirname "$0")"
#PID_FILE="$AIRFLOW_HOME/service_pids.txt"
#
#if [[ -f "$PID_FILE" ]]; then
#    echo "Airflow Service Status:"
#    echo "======================"
#    source "$PID_FILE"
#
#    for service in WEBSERVER SCHEDULER DAG_PROCESSOR TRIGGERER; do
#        pid_var="${service}_PID"
#        pid=${!pid_var}
#        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
#            echo "✓ $service (PID: $pid) - Running"
#        else
#            echo "✗ $service - Not running"
#        fi
#    done
#
#    echo ""
#    echo "Log files location: $AIRFLOW_HOME/logs/services/"
#    echo "Web UI: http://localhost:8080"
#else
#    echo "No PID file found. Services are not running."
#fi
#EOF
#
#    # Make scripts executable
#    chmod +x "$AIRFLOW_HOME/stop_airflow.sh"
#    chmod +x "$AIRFLOW_HOME/status_airflow.sh"
#
#    echo "Management scripts created:"
#    echo "  Stop services: $AIRFLOW_HOME/stop_airflow.sh"
#    echo "  Check status:  $AIRFLOW_HOME/status_airflow.sh"
#}
#
## Start services
#start_services
#
## Create management scripts
#create_management_scripts
#
#echo ""
#echo "=== Installation Complete ==="
#echo "Airflow has been installed in: $AIRFLOW_HOME"
#echo ""
#echo "Access the web UI at: http://localhost:8080"
#echo "Username: admin"
#echo "Password: Check the output above for the generated password"
#echo ""
#echo "Service Management:"
#echo "  • Check status: $AIRFLOW_HOME/status_airflow.sh"
#echo "  • Stop services: $AIRFLOW_HOME/stop_airflow.sh"
#echo "  • View logs: $AIRFLOW_HOME/logs/services/"
#echo ""
#echo "All services are now running in the background!"