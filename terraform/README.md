## Setup
in default folder, run terraform init once to set everything up.

### Usage
Something something terraform apply / terraform destroy

### Credentials
Locally, make sure this environment variable is set to your google credentials file:
```export GOOGLE_APPLICATION_CREDENTIALS=${path to your file}```

[Create and download a new credentials json](https://console.developers.google.com/apis/credentials?organizationId=802489480189&project=esoteric-helix-197319)

### SSH USER
By default, the terraform command will ask for the ssh user to use. If you'd prefer to avoid answering this every time, you can set the TF_VAR_ssh_user env variable.
```export TF_VAR_ssh_user=${USER}``` is probably what you want.


