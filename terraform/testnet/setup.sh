#!/usr/bin/env bash
set -e

sudo mkdir -p /home/ubuntu/constellation
sudo /usr/bin/gsutil cp gs://constellation-dag/release/dag-1.0.1.jar /home/ubuntu/constellation/dag.jar

sudo cp /tmp/start /home/ubuntu/constellation/start
sudo cp /tmp/constellation.service /etc/systemd/system/constellation.service
sudo cp /tmp/dag /etc/init.d/dag
sudo chmod 0755 /etc/init.d/dag
sudo chmod u+x /home/ubuntu/constellation/start
sudo curl "http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" -H "Metadata-Flavor: Google" > /tmp/external_host_ip
sudo cp /tmp/external_host_ip /home/ubuntu/constellation/external_host_ip
sudo chown -R ubuntu:ubuntu /home/ubuntu/constellation

sudo systemctl daemon-reload
sudo systemctl enable constellation.service
