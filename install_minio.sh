#!/bin/bash

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

# Install MinIO python module
if pip list | grep minio > /dev/null;then
    echo "minio in pip list"
else
    pip install minio
    echo "minio installed with pip"
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

exit 0
