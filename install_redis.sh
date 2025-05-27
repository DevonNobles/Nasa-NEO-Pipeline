#!/bin/bash

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

if ! pip list | grep redis; then
  pip install redis
fi