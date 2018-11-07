#!/usr/bin/env bash

# https://cloud.google.com/sdk/gcloud/reference/compute/instances/create
# https://cloud.google.com/sdk/gcloud/reference/compute/firewall-rules/create
NUM_MACHINES=${1:-1}
NAME_PREFIX=${2:-dev}
OFFSET=${3:-0}

ZONES=(us-west2-b europe-north1-b asia-northeast1-b australia-southeast1-b northamerica-northeast1-b asia-south1-b)

NUM_MACHINES_M1=$(($NUM_MACHINES - 1))

for i in `seq 0 $NUM_MACHINES_M1`
do
    J=$((i+$OFFSET))
    echo "Offset $J"
    NAME="$NAME_PREFIX-$J"
    CHOSEN_ZONE=${ZONES[$J]}
    echo $NAME $CHOSEN_ZONE
    gcloud compute --project "esoteric-helix-197319" disks create $NAME --size "100" --zone $CHOSEN_ZONE --source-snapshot "dev" --type "pd-standard"

    gcloud beta compute --project=esoteric-helix-197319 instances create $NAME --zone=$CHOSEN_ZONE --machine-type=n1-standard-1 \
    --subnet=default --network-tier=PREMIUM --metadata=group=dev --maintenance-policy=MIGRATE \
    --service-account=898183181620-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --tags=bad-practices,http-server,https-server --disk=name=$NAME,device-name=$NAME,mode=rw,boot=yes,auto-delete=yes --labels=group=$NAME_PREFIX
done