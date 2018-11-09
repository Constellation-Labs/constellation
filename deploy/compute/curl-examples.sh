#!/usr/bin/env bash


curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"host":"35.197.16.154","port": 9001}' \
  http://35.190.167.41:9000/peer/add


curl \
  --request POST \
  http://35.190.167.41:9000/download/start


curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"dst": "DAG6zHxVPKYXBeCd6BhKFFFokWAWnsorxuztgSVY", "amount": 555545, "normalized": true}' \
  http://35.185.232.16:9000/send

