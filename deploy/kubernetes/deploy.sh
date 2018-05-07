#!/usr/bin/env bash

GOOGLE_PROJECT_ID="esoteric-helix-197319"
GOOGLE_CLUSTER_NAME="constellation-test"
IMAGE=gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$CIRCLE_SHA1

sudo /opt/google-cloud-sdk/bin/kubectl patch statefulset constellation-app --type='json' \
-p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"'$IMAGE'"}]'

sudo /opt/google-cloud-sdk/bin/kubectl rollout status sts constellation-app