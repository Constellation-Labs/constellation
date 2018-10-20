#!/usr/bin/env bash

# https://cloud.google.com/sdk/gcloud/reference/compute/instances/create
# https://cloud.google.com/sdk/gcloud/reference/compute/firewall-rules/create
NAME_PREFIX=${1:-dev}
NUM_MACHINES=${2:-1}
OFFSET=${3:-0}

for i in `seq 1 $NUM_MACHINES`
do
    J=$((i+$OFFSET))
    echo "Offset $J"
    NAME="$NAME_PREFIX-$J"
    echo $NAME
    gcloud compute --project "esoteric-helix-197319" disks create $NAME --size "100" --zone "us-east1-b" --source-snapshot "dev" --type "pd-standard"

    gcloud beta compute --project=esoteric-helix-197319 instances create $NAME --zone=us-east1-b --machine-type=n1-standard-1 \
    --subnet=default --network-tier=PREMIUM --metadata=group=dev --maintenance-policy=MIGRATE \
    --service-account=898183181620-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --tags=bad-practices,http-server,https-server --disk=name=$NAME,device-name=$NAME,mode=rw,boot=yes,auto-delete=yes --labels=group=$NAME_PREFIX
done