#/bin/env sh
set -e
CLUSTER_TAG=`terraform output cluster_tag`
gcloud --format="value(networkInterfaces[0].accessConfigs[0].natIP)" compute instances list --filter="tags:$CLUSTER_TAG"
