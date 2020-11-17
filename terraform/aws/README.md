# AWS cluster

## Preconditions
- Terraform v0.12
- pssh

## Starting a cluster

In following commands `abcd` is a workspace name, you should type your own name.

```sh
$ terraform workspace new abcd
Created and switched to workspace "abcd"!
```

Now, you can either use `autostart` or manually run commands step by step, depending on the use case.

### Autostart
 
Use `autostart` which assembles the project and starts a cluster automatically. You have to provide a number of instances.

```sh
$ ./dag autostart 10
```

If you want to re-use `$HOSTS_FILE`, use following command to export them automatically:

```sh
$ export HOSTS_FILE=$(./dag exportIPs)
``` 

If you want to run commands in a non-interactive way, use the `--batch` or `-b` option:

```shell script
$ ./dag autostart 10 --batch
``` 

### Step by step:

You have to assemble the project and upload a .jar file to an s3 bucket.

```sh
$ ./dag assemble
$ ./dag upload
```

Once the .jar file is uploaded to the s3 bucket, you have to launch all instances.
You can type manually how many instances you'd want to launch (default=3).

```sh
$ terraform apply -var 'instance_count=5'
```

Terraform apply will require confirmation (type `yes`) (you can use `-auto-approve` for auto approval).

After successful approval you have to set the `$HOSTS_FILE` environment variable.

```sh
$ export HOSTS_FILE=$(./dag exportIPs)
```

Then, you are able to launch nodes. Use the following command:

```sh
$ ./dag start
```

## Restarting and redeploying

To redeploy a cluster, just use following command:

```sh
$ ./dag redeploy
````

To stop cluster nodes (i.e. if you want to stop creating new logs and take a look inside)

```sh
$ ./dag stop
```

To manually upload a jar file or fetch it on instances:
```sh
$ ./dag upload
$ ./dag updateJar
```

## Destroying a cluster

**First of all, be sure you are on the proper workspace!**

```sh
$ terraform workspace show
abcd
```

Then, feel free to destroy the cluster (approval required)

```sh
$ terraform destroy
```

## Switch workspaces

If you want to switch the workspace (i.e. you want to spin up a second cluster), you can create a new one or check for available workspaces

```sh
$ terraform workspace list
  default
* abcd
  test
```

and select another one:

```sh
$ terraform workspace switch test 

$ terraform workspace show
test
```

## Change cluster size

When you need to reduce or increase the cluster size, you are able to use terraform apply with a different configuration.
Mind that each time you're running the terraform apply, you have to run it with the same amount of instances. Otherwise, it will reduce your cluster to the default value (3 instances).

```sh
$ terraform apply -var 'instance_count=10'
```