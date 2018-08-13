# Constellation
A blockchain microservice operating system.

This is the reference implementation supporting our 
[DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) 
protocol, writting in the 
[Scala](https://www.scala-lang.org/) 
programming language.

## Building for development
#### Instructions tested on Mac and Linux (may work elsewhere)
1. Check out repository 
```
git clone git@github.com:Constellation-Labs/constellation.git
```
2. From root directory ``constellation, run 
```
./build.sh
```
or optionally (to connect to other host)
```
./build.sh seedhost:port
```
3. Interact with app via the API 
By default this is 
http://localhost:9000. 
Look at API.scala for endpoints.

## Docker execution 
This is assuming you have docker already set up.

##### Install docker locally
We make use of the [Docker](https://www.docker.com/) toolset.

*For now this installation is not covered here in detai*

**We will publish docker images to dockerhub soon(ish).**

1. Follow directions for building for development (other than step 4).
2. Run
```
sbt docker:publishLocal
```
3. Run
```
./run-local-docker.sh
```

For windows, just grab the docker ```cmd``` from the file and run directly.

### Vagrant execution
This is **deprecated** but may be useful for people running Windows, etc.:
1. Download [vagrant](https://www.vagrantup.com).
2. Run 
```
vagrant up
``` 
from project directory.

## API Docs
#### WIP 
We intend to use **Swagger** or similar to publish comprehensive API docs.

### Troubleshooting :rotating_light:
If you run into issues during the installation, best make a thread on the [community portal Orion](https://orion.constellationlabs.io/accounts/login/?next=/) or ask a quick question on the Constellation [Discord server](http://Discordapp.com/).

If you discover a bug, please open a ticket using the github issue function. If you don't know the resolution, follow standar github reporting guidelines, stating your system, what you did and tried. 

 Note: For software support, please ask on discord first.

#### Community write-ups :two_hearts: 
1. [Vagrant setup for windows](https://drive.google.com/file/d/1xobpv4Ew1iCN9j-M-ItU6PsfnybHUryy/view)
2. [Vagrant setup for ubuntu](https://docs.google.com/document/u/1/d/e/2PACX-1vST7vBIMxom99hKr5XyVFpM6TAs_pw-iqq403AktMWnqr3dxUFX5c0g9BWD5gU5TDPZVXKcW3HTWbVl/pub)

We welcome community documentation and tutorials. You can share them on the [community portal Orion](https://orion.constellationlabs.io/accounts/login/?next=/) and on the Constellation [Discord server].
