# Constellation
Decentralized Application Integration Platform.

This repository is the reference implementation of our 
[DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) 
protocol using the 
[Scala](https://www.scala-lang.org/) 
programming language.

```
constellation
├── scripts                                <─── shell scripts
└── src
    ├── it                                 <─── continuous integration
    ├── main/scala/org/constellation
        ├── consensus                      <─── consensus algorithm implementation
        ├── crypto
        ├── API.scala
        └── LevelDB.scala
    └── test                               <─── unit tests
├── build.sh
└── run-local-docker.sh
```

## :computer: Building for development
### On Mac and Linux
1. Check out repository 
```haskell
git clone git@github.com:Constellation-Labs/constellation.git
```
2. From root directory ``constellation, run 
```haskell
./build.sh
```
or optionally (to connect to other host)
```haskell
./build.sh seedhost:port
```
3. Interact with app via the API 
By default this is 
http://localhost:9000. 
Look at the file _API.scala_ for endpoints.

### Docker execution 
This is assuming you have docker already set up.

#### Install docker locally
We make use of the [docker](https://www.docker.com/) toolset.

Note: *For now this installation is not covered here in detai*

**We will publish docker images to dockerhub soon(ish).**

1. Follow directions for building for development (other than step 4).
2. Run
```haskell
sbt docker:publishLocal
```
3. Run
```haskell
./run-local-docker.sh
```

For windows, just grab the docker ```cmd``` from the file and run directly.

### Vagrant execution
This is **deprecated** but may be useful for people running Windows, etc.:
1. Download [vagrant](https://www.vagrantup.com).
2. Run 
```haskell
vagrant up
``` 
from project directory. See also
* [Vagrant setup for windows](https://drive.google.com/file/d/1xobpv4Ew1iCN9j-M-ItU6PsfnybHUryy/view)
* [Vagrant setup for ubuntu](https://docs.google.com/document/u/1/d/e/2PACX-1vST7vBIMxom99hKr5XyVFpM6TAs_pw-iqq403AktMWnqr3dxUFX5c0g9BWD5gU5TDPZVXKcW3HTWbVl/pub)

## :green_book: API Docs
### WIP 
We intend to use **Swagger** or similar to publish comprehensive API docs.

## :rotating_light: Troubleshooting
If you run into issues during the installation, best make a thread on the [community portal **Orion**](https://orion.constellationlabs.io/accounts/login/?next=/) or ask a quick question on the Constellation [discord server](http://Discordapp.com/).

Should you discover a bug, please open a ticket using the github issue function. If you don't know the resolution, follow standar github reporting guidelines, stating your system, what you did and tried. 

For software support and contributions :two_hearts:, you can find links to the community outlets as well as more documentation and developer tools in the 
[**resource list**](https://github.com/Constellation-Labs/awesome-constellation). 
