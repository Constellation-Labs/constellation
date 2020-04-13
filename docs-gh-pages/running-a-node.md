# Running a node

## Download release
Releases are currently distributed as Java JAR files. 
Download the [latest from github](https://github.com/Constellation-Labs/constellation/releases) or from 
[Dockerhub.](https://hub.docker.com/repository/docker/constellationprotocol/node) It can be run on any system 
supporting Java 8 or higher.

## Node Requirements
To run a constellation node, you'll need a machine with java 8 or higher installed.
While we expect that running on Windows will work, we have only internally tested on MacOS and Linux.

### Instance specs:
We haven't spent much time optimizing for resource usage, so these specs are higher than we anticipate at mainnet.

Currently, for testnet, we recommend:

* CPU: 2-core minimum.
* Memory: 3GB or higher
* Disk: 200GB available.

## Node Configuration
Nodes can be configured in two ways. Some options are available through the cmdline, and more extensive options are available by supplying a configuration file. There is a 
[default configuration file](https://github.com/Constellation-Labs/constellation/blob/dev/src/main/resources/reference.conf), 
but the options in it can be overriden by providing an additional conf file. The format is HOCON from 
[Lightbend Config](https://github.com/lightbend/config) 
but is essentially a superset of YAML & JSON. We intend to document available configuration options in more depth soon.

### Port Configuration
A constellation node has two API's: A control API meant for the node operator to make changes to node operation, and a data (or peer) API, used by nodes to communicate with each other.

By default, the control API uses port 9000, and the peer API uses port 9001. These can be overridden in the node configuration, but whatever they are set to, these ports must be open and exposed to the public internet.

## Where to Run
### Running a node at home
While running a from your home is possible, it's not a supported or recommended setup. It can be difficult to properly 
expose ports on your computer to the public internet, because home networking equipment like modems and routers often 
have firewalls blocking these ports, and often home networks do not have a stable ip address. For now, our official 
recommendation is to use a cloud provider.

### Cloud hosting
Walkthrough for [GCP](https://docs.google.com/document/u/0/d/1KkJSdydz2DsAlQKaVB8MVqqQuPMr-ggTB2Y2niKOodY/mobilebasic)
Walkthrough for [AWS/DigitalOcean and Docker](https://docs.google.com/document/d/1qFYLqv2g_gHWWFs-x9YsPvrGzEocMPZS9hsJBegPrz0/edit)

## Manual Builds And Deployment
The [build instructions](https://github.com/Constellation-Labs/constellation/blob/dev/docs/build-instructions.md) 
have more pointers on how to run a node. 

### Additional Tools
The DAG [Command Line Tool](https://github.com/zaemliss/Constellation)

## Connecting To An Existing Network
Be sure that you have the latest whitelist, which can be found by running 
``wget https://github.com/Constellation-Labs/constellation/releases/latest/download/whitelisting``
And you can update the whitelist with your info on [this spreadsheet](https://docs.google.com/spreadsheets/d/1MGBevI3MbhsN-oueC_q8ZPKRpWdPyaITcJpAhz60lPo/edit?pli=1#gid=0)
### Send join request
``curl -s -X POST http://YOURNODE/join -H "Content-type: application/json" -d "{ \"host\": \"DESTINATIONNODE\", \"port\": 9001 }"``