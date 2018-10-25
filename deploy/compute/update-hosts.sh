#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts.txt}
GROUP=${2-dev}

#gcloud --format=json compute instances list --filter="labels.group=dev"
# --filter=name:instance-1
gcloud --format="value(networkInterfaces[0].accessConfigs[0].natIP)" \
compute instances list --filter="labels.group=$GROUP" > $HOSTS_FILE