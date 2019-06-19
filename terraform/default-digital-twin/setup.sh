#!/usr/bin/env bash
set -e

mkdir -p /tmp/constellation
gsutil cp gs://constellation-dag/release/dag-digital-twin.jar /tmp/constellation/dag.jar
curl "http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" -H "Metadata-Flavor: Google" > /tmp/constellation/external_host_ip
cp /tmp/start /tmp/constellation/start

sudo mkdir -p /home/ubuntu
sudo mv /tmp/constellation /home/ubuntu/constellation
sudo cp /tmp/constellation.service /etc/systemd/system/constellation.service
sudo cp /tmp/dag /etc/init.d/dag
sudo chmod 0755 /etc/init.d/dag
sudo chmod u+x /home/ubuntu/constellation/start
sudo chown -R ubuntu:ubuntu /home/ubuntu/constellation

sudo systemctl daemon-reload
sudo systemctl enable constellation.service
