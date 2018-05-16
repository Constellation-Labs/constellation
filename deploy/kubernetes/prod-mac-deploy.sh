#!/usr/bin/env bash

GOOGLE_PROJECT_ID="esoteric-helix-197319"
GOOGLE_CLUSTER_NAME="constellation-test"
DATE=$(date +%s)

# This is required because unless the YAML changes a value it won't trigger a redeploy to a new docker image..
IMAGE_TAG=$USER-$DATE
IMAGE=gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$IMAGE_TAG
echo "Using docker image: $IMAGE"

sbt docker:publishLocal
docker tag constellationlabs/constellation:latest $IMAGE
gcloud docker -- push $IMAGE

APP_USER="constellation-app"

kubectl patch statefulset $APP_USER --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"'$IMAGE'"}]'

kubectl rollout status sts $APP_USER

sbt it:test