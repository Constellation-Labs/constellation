Ensure

```
Host * 
StrictHostKeyChecking no
```

Present in `~/.ssh/config` before running pssh commands.

We're using GCP Compute instances temporarily instead of Kubernetes due to low entropy bug.

Workflow:

`create-cluster-from-snapshot.sh <num-machines> <node-label>`

for bringing up machines

`update-hosts.sh <hosts-file> <node-label>`

`redeploy.sh <hosts-file> <node-label>`

Checking active cluster after initialization (requires hosts file in one of locations:

- environmental variable HOSTS_FILE
- <project_root>/hosts-2.txt
- <project_root>/terraform/*/hosts


`sbt "it:testOnly org.constellation.ClusterHealthCheckTest"`