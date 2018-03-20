#!/usr/bin/env bash

export CHANGE_MINIKUBE_NONE_USER=true; sudo minikube start --vm-driver none

# this for loop waits until kubectl can access the api server that Minikube has created
for i in {1..150}; do # timeout for 5 minutes
   ./kubectl get po &> /dev/null
   if [ $? -ne 1 ]; then
      break
  fi
  sleep 2
done

# doing this temporarily since there is currently an issue with minikube init
# https://github.com/kubernetes/minikube/issues/2479
sudo minikube delete
sudo minikube start --vm-driver none

# this for loop waits until kubectl can access the api server that Minikube has created
for i in {1..150}; do # timeout for 5 minutes
   ./kubectl get po &> /dev/null
   if [ $? -ne 1 ]; then
      break
  fi
  sleep 2
done

sudo kubectl config use-context minikube
sudo kubectl proxy --port=8080 &
