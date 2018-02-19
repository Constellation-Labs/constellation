vagrant ssh -c 'sudo kubectl proxy --port=8080 &'
vagrant ssh -c 'export CHANGE_MINIKUBE_NONE_USER=true; sudo minikube start --vm-driver none'
vagrant ssh -c 'sudo kubectl config use-context minikube'
