Ensure


Host *
    StrictHostKeyChecking no

Present in ~/.ssh/config before running pssh commands


Using GCP Compute instances temporarily instead of Kubernetes due to low entropy bug

Workflow:

create-cluster-from-snapshot.sh <num-machines> <node-label> for bringing up machines

update-hosts.sh <hosts-file> <node-label>

redeploy.sh <hosts-file> <node-label>