#!/usr/bin/env bash


#gcloud --format=json compute instances list --filter="labels.group=dev"
# --filter=name:instance-1
gcloud --format="value(networkInterfaces[0].accessConfigs[0].natIP)" compute instances list --filter="labels.group=dev" > hosts.txt
