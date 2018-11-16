## Setup
Some steps specific to GCP (Google Cloud Platform). For other platforms, this will require changes.

```bash
brew install terraform
brew install jq
```

### Credentials
Locally, make sure this environment variable is set to your google credentials file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=${path to your file}
```

Use your own GCP project (if running a test node outside Constellation org).
If you don't have a service account, make one with compute engine access. Otherwise, make a new credentials json file by creating a new service account key and download it to your local machine.
Here is how to create and download a new credentials json: 
[Link to Google Cloud API/Credentials](https://console.developers.google.com/apis/credentials?organizationId=802489480189&project=esoteric-helix-197319).

### SSH USER
*Make sure you have the following exectued, otherwise you will break the cluster!*

```bash
ssh-add ~/.ssh/id_rsa
```

Terraform requires agent forwarding to pick up the keys, it will break the machines due to overwhelming `sshd` slots.

By default, the terraform command will ask for the ssh user to use. 
If you'd prefer to avoid answering this every time, you can set the TF_VAR_ssh_user env variable.

```bash
export TF_VAR_ssh_user=${USER}
```

is probably what you want.

### Initialize
```bash
cd to terraform/default
```

or copy paste and change settings, make sure to change terraform.backend.prefix in `main.tf`.

If you have a pre-existing state already (after copying folder with new name), say 'no' when the prompt asks to copy the state. And then run

```bash
terraform init # to setup plugins

terraform show # to see existing state (or if it's a new folder should return empty)
```

### Usage
_Something something terraform apply / terraform destroy_

```bash
terraform show # after the cluster is online, to verify it's working

./ips_to_hosts_file.sh # to grab ips
```
