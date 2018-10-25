#!/usr/bin/env bash

# https://cloud.google.com/sdk/gcloud/reference/compute/instances/create
# https://cloud.google.com/sdk/gcloud/reference/compute/firewall-rules/create
# TODO: Snapshot docs for making snapshot afterwards

# This still requires modifications, was the original command used for building snapshot
# But assumes a firewall rule has already been created

gcloud beta compute --project=esoteric-helix-197319 instances create dev-0 --zone=us-east1-b --machine-type=n1-standard-1 --subnet=default --network-tier=PREMIUM --metadata=group=dev --maintenance-policy=MIGRATE --service-account=898183181620-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append --tags=bad-practices,http-server,https-server --image=ubuntu-1804-bionic-v20181003 --image-project=ubuntu-os-cloud --boot-disk-size=100GB --boot-disk-type=pd-standard --boot-disk-device-name=dev-1
