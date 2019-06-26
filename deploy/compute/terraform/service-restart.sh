#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts-dev.txt}

echo "Restarting $HOSTS_FILE"
cat $HOSTS_FILE

pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i 'sudo rm -rf /home/ubuntu/constellation/tmp'
pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i "sudo systemctl restart constellation"

echo "Done restarting"