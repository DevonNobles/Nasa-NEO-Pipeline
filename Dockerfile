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
    lsb-release\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create ubuntu user with sudo privileges
RUN usermod -aG sudo ubuntu && \
    echo "ubuntu ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Create /app directory and set ownership
RUN mkdir -p /app && chown ubuntu:ubuntu /app

# Set working directory
WORKDIR /app

# Copy project files
COPY --chown=ubuntu:ubuntu . /app/

# Create Python virtual environment and install dependencies
USER ubuntu
RUN python3 -m venv .venv && \
    .venv/bin/pip install --upgrade pip && \
    if [ -f requirements.txt ]; then .venv/bin/pip install -r requirements.txt; fi

# Activate virtual environment by default
ENV PATH="/app/.venv/bin:$PATH"

# Expose ports for web services
EXPOSE 8080 9001 8888

# Set default command to run entrypoint script if it exists, otherwise bash
CMD ["/bin/bash", "-c", "./entrypoint.sh ; tail -f /dev/null"]