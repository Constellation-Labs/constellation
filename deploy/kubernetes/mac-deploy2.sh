#!/usr/bin/env bash

GOOGLE_PROJECT_ID="esoteric-helix-197319"
GOOGLE_CLUSTER_NAME="constellation-test"
IMAGE_TAG=latest
IMAGE=gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$IMAGE_TAG

sbt docker:publishLocal
docker tag constellationlabs/constellation:latest $IMAGE
gcloud docker -- push $IMAGE

kubectl delete pod constellation-app-1-0
kubectl delete pod constellation-app-2-0
kubectl delete pod constellation-app-3-0

#sbt it:test