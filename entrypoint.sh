#!/bin/bash

# Unicode cheat sheet
# ✓ CTRL + SHIFT + U 2713
# ✗ CTRL + SHIFT + U 2717
# ⚠ CTRL + SHIFT + U 26A0

set -e # fail early

# Set up minio user and group
# Add minio-user group only if it doesn't exist
getent group minio-user > /dev/null || sudo groupadd -r minio-user

# Add minio-user user only if it doesn't exist
id -u minio-user &> /dev/null || sudo useradd -M -r -g minio-user minio-user

# Create data directory only if it doesn't exist
[ -d /mnt/data ] || sudo mkdir -p /mnt/data

# Change ownership only if needed
[ "$(stat -c '%U:%G' /mnt/data 2>/dev/null)" = "minio-user:minio-user" ] || sudo chown minio-user:minio-user /mnt/data

# Install minio server
if minio --version &> /dev/null; then
  echo "minio server already installed"
  echo "Version: $(minio --version)"
else
  echo "Downloading deb package...."
  wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio_20250422221226.0.0_amd64.deb -O minio.deb
  echo "Installing minio server deb package..."
  sudo dpkg -i minio.deb
  rm minio.deb
  sudo tee /etc/default/minio > /dev/null << EOF
    # MINIO_ROOT_USER and MINIO_ROOT_PASSWORD sets the root account for the MinIO server.
    # This user has unrestricted permissions to perform S3 and administrative API operations on any resource in the deployment.
    # Omit to use the default values 'minioadmin:minioadmin'.
    # MinIO recommends setting non-default values as a best practice, regardless of environment

    # MINIO_ROOT_USER=myminioadmin
    # MINIO_ROOT_PASSWORD=minio-secret-key-change-me

    # MINIO_VOLUMES sets the storage volume or path to use for the MinIO server.

    MINIO_VOLUMES="/mnt/data"

    # MINIO_OPTS sets any additional commandline options to pass to the MinIO server.
    # For example, \`--console-address :9001\` sets the MinIO Console listen port
    MINIO_OPTS="--console-address :9001"
EOF
fi

# Install miniIO Client
if command -v mc &> /dev/null; then
  echo "minioIO Client already installed"
else
  curl https://dl.min.io/client/mc/release/linux-amd64/mc \
    --create-dirs \
    -o "$PWD/minio/minio-binaries/mc"

  chmod +x "$PWD/minio/minio-binaries/mc"
  sudo ln -sf "$PWD/minio/minio-binaries/mc" /usr/local/bin/mc
  export PATH=$PATH:$PWD/minio/minio-binaries/
  echo "MinIO Client installed"
  echo "Use 'mc --help' for more info"
fi


sudo systemctl start minio.service
if [ "$(systemctl is-active minio.service)" = "active" ]; then
  echo "minio.service is $(systemctl is-active minio.service)"
  echo "username: minioadmin"
  echo "password: minioadmin"
  echo "Url: http://localhost:9001/"
else
  echo "minio.service is $(systemctl is-active minio.service)"
  exit 1
fi

# Set alias
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc ready myminio

#Create buckets
mc mb --ignore-existing myminio/neo

# Install redis
if ! redis-cli --version; then
  # Add redis repository to the APT index
  sudo apt-get install lsb-release curl gpg
  curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
  sudo chmod 644 /usr/share/keyrings/redis-archive-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

  # Update APT index and install Redis
  sudo apt-get update
  sudo apt-get install redis
fi


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
if [ ! "$(pyiceberg --help &> /dev/null ; echo $?)" = "0" ];then
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

# Install Panel python module
if pip list | grep panel > /dev/null;then
    echo "Panel in pip list"
else
    pip install panel
    echo "Panel installed with pip"
fi

# Install hvplot python module
if pip list | grep hvplot > /dev/null;then
    echo "hvplot in pip list"
else
    pip install hvplot
    echo "hvplot installed with pip"
fi


# Create Airflow Start script
cat > "$PWD/start_airflow.sh" << 'OUTER_EOF'
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

# Copy daily_nasa_neo_etl_pipeline.py to dag folder
cp "src/daily_nasa_neo_etl_pipeline-TEMPLATE.py" "$AIRFLOW_HOME/dags/daily_nasa_neo_etl_pipeline.py"

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
OUTER_EOF

  # Make script executable
  chmod +x "$PWD/start_airflow.sh"
  echo "Start Airflow script created: $PWD/start_airflow.sh"

# Remove minio directory
rm -r minio