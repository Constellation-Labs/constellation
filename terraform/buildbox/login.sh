IP=`terraform output -json instance_ips | jq ".[]" -r`
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $IP