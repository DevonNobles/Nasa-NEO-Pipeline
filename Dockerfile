FROM ubuntu:latest

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Update package list and install required packages
RUN apt-get update && \
    apt-get install -y \
    sudo \
    curl \
    wget \
    python3 \
    python3-pip \
    python3-venv \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create ubuntu user with sudo privileges
RUN useradd -m -s /bin/bash ubuntu && \
    usermod -aG sudo ubuntu && \
    echo "ubuntu ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Set working directory
WORKDIR /app

# Copy project files
COPY --chown=ubuntu:ubuntu . /app/

# Create Python virtual environment and install dependencies
USER ubuntu
RUN python3 -m venv venv && \
    . venv/bin/activate && \
    pip install --upgrade pip && \
    if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Activate virtual environment by default
ENV PATH="/app/venv/bin:$PATH"

# Expose ports for web services
EXPOSE 8080 9001 8888

# Set default command to run entrypoint script if it exists, otherwise bash
CMD ["/bin/bash", "-c", "./entrypoint.sh ; ./start_airflow.sh"]