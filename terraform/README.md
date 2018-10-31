## Setup

(Some steps specific to GCP, requires changes for other platforms)

brew install terraform
brew install jq

### Credentials

Locally, make sure this environment variable is set to your google credentials file:
```export GOOGLE_APPLICATION_CREDENTIALS=${path to your file}```
[Create and download a new credentials json](https://console.developers.google.com/apis/credentials?organizationId=802489480189&project=esoteric-helix-197319)
Use your own GCP project (if running a test node outside Constellation org)
If you don't have a service account, make one with compute engine access,
otherwise, make a new credentials json file by creating a new service account key and download it to your local machine


### SSH USER
By default, the terraform command will ask for the ssh user to use. If you'd prefer to avoid answering this every time, you can set the TF_VAR_ssh_user env variable.
```export TF_VAR_ssh_user=${USER}``` is probably what you want.


#Initialize

cd to terraform/default (or copy paste and change settings) and then run
terraform init # to setup plugins
terraform show # after the cluster is online

to verify it's working.


in default folder, run terraform init once to set everything up.

### Usage
Something something terraform apply / terraform destroy


