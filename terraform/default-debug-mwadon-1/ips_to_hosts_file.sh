#!/usr/bin/env sh
terraform output -json instance_ips | jq ".value[]" -r
