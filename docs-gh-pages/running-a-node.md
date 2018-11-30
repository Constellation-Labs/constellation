# Running a node
Constellation nodes are currently released as JAR file. These can be run on any system supporting java 8 or higher.

## Download release
Releases are currently distributed as Java JAR files. [Download the latest from github.](https://github.com/Constellation-Labs/constellation/releases)

## Node Requirements
To run a constellation node, you'll need a machine with java 8 or higher installed.
While we expect that running on Windows will work, we have only internally tested on MacOS and Linux.

### Instance specs:
We haven't spent much time optimizing for resource usage, so these specs are higher than we anticipate at mainnet.

Currently, for testnet, we recommend:

* CPU: 2-core minimum.
* Memory: 3GB or higher
* Disk: 200GB available.

## Connecting To An Existing Network
The most important configuration to connect to our testnet is providing a seed node to connect to. The easiest way to do this right now is to set it with an environment variable (example ip only, don't use):

```export DAG_SEED_PEER=123.456.123.456:9001```

We are not currently running any publicly joinable network -- please get in touch on Telegram or Discord if you'd like to connect a node to our testnet network.

## Node Configuration
Nodes can be configured in two ways. Some options are available through the cmdline, and more extensive options are available by supplying a configuration file. There is a [default configuration file](https://github.com/Constellation-Labs/constellation/blob/dev/src/main/resources/reference.conf), but the options in it can be overriden by providing an additional conf file. The format is HOCON from [Lightbend Config](https://github.com/lightbend/config) but is essentially a superset of YAML & JSON. We intend to document available configuration options in more depth soon.

### Port Configuration
A constellation node has two API's: A control API meant for the node operator to make changes to node operation, and a data (or peer) API, used by nodes to communicate with each other.

By default, the control API uses port 9000, and the peer API uses port 9001. These can be overridden in the node configuration, but whatever they are set to, these ports must be open and exposed to the public internet.

## Where to Run
As a team we have the most expertise with [Google Cloud](https://cloud.google.com/), and it is our recommended platform for those without one already, but any cloud provider will work.


### Cloud providers
Any cloud provider should work fine. While we do all our testing on GCP (Google Cloud Provider), we are not using any proprietary features. If you can launch a machine, install java 8 or higher, assign an external IP, and open the ports for the control API and peer API, you shouldn't have any problems.


### Running a node at home
While running a from your home internet is possible, it's not a supported or recommended setup. It can be difficult to properly expose ports on your computer to the public internet, because home networking equipment like modems and routers often have firewalls blocking these ports, and often home networks do not have a stable ip address. For now, our official recommendation is to use a cloud provider.

## Additional Tools

### Docker
We intend to have official docker images soon -- stay tuned.

### Terraform (Optional!)
At Constellation Labs we use terraform to quickly launch and destroy clusters for testing. Our terraform configurations are checked in to the repository in the terraform directory, so they are available as a guide if you'd like to use it yourself. They automate some nice but optional things like running the node as a service. They are currently setup for GCP, and they have a few constellation-specific hardcoded variables (backend storage location, project name). That said -- if you're familiar with terraform, they should be straightforward to adapt for your uses. While we are not officially supporting this right now, we can provide some support on discord for this method.