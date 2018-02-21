vagrant ssh -c 'export CHANGE_MINIKUBE_NONE_USER=true; sudo minikube start --vm-driver none'
vagrant ssh -c '
# this for loop waits until kubectl can access the api server that Minikube has created
for i in {1..150}; do # timeout for 5 minutes
   ./kubectl get po &> /dev/null
   if [ $? -ne 1 ]; then
      break
  fi
  sleep 2
done'
# doing this temporarily since there is currently an issue with minikube init
# https://github.com/kubernetes/minikube/issues/2479
vagrant ssh -c 'sudo minikube delete'
vagrant ssh -c 'sudo minikube start --vm-driver none'
vagrant ssh -c '
# this for loop waits until kubectl can access the api server that Minikube has created
for i in {1..150}; do # timeout for 5 minutes
   ./kubectl get po &> /dev/null
   if [ $? -ne 1 ]; then
      break
  fi
  sleep 2
done'
vagrant ssh -c 'sudo kubectl config use-context minikube'
vagrant ssh -c 'sudo kubectl proxy --port=8080 &'
