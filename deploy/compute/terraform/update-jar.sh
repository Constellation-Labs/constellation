#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts-dev.txt}
JAR_TAG=${2:-dev}

pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i "sudo gsutil cp gs://constellation-dag/release/dag-$JAR_TAG.jar /home/ubuntu/constellation/dag.jar" && \
pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i "sudo chown -R ubuntu:ubuntu /home/ubuntu/constellation"

echo "Updated jar on remote"