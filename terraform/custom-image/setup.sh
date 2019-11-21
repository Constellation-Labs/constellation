#!/usr/bin/env bash
set -e

mkdir -p /tmp/constellation
gsutil cp gs://constellation-dag/release/dag-dev.jar /tmp/constellation/dag.jar
curl "http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" -H "Metadata-Flavor: Google" > /tmp/constellation/external_host_ip
cp /tmp/start /tmp/constellation/start
cp /tmp/logback.xml /tmp/constellation/logback.xml

sudo mv /tmp/constellation /home/ubuntu/constellation
sudo chmod u+x /home/ubuntu/constellation/start
sudo chown -R ubuntu:ubuntu /home/ubuntu/constellation

sudo systemctl daemon-reload
sudo systemctl enable constellation.service
