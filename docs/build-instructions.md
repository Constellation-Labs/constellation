## Build instructions
### Running a node in the Cloud
Most operators host the node in the 
[Google Cloud Platform](https://cloud.google.com/) 
(GCP) *compute engine* using a Ubuntu or Debian machine. Make sure the relevant ports are open / not firewalled. 

**Minimal specs**
```haskell
    CPU:       2-core minimum
    Memory:    3 GB or higher
    Disk:      50-200 GB available
    Software:  Java 8.x
```

You find the 
[**documentation** (website)](https://constellation-labs.github.io/constellation/running-a-node/).
This variant uses the latest JAR file from the 
[constellation/releases](https://github.com/Constellation-Labs/constellation/releases)
page. 
More is coming, as well as more support for docker and terraform.

(TODO: make these as well as the webpage docs read their content from a common file)

### :computer: On Linux and Mac
1. Check out the repository

```bash
git clone git@github.com:Constellation-Labs/constellation.git
cd constellation
```

2. From root directory `constellation`, run 

```bash
./build.sh
```

or optionally (to connect to other host)

```bash
./build.sh seedhost:port
```

3. Interact with app via the API 
By default this is `http://localhost:9000`. Look at the file _API.scala_ for endpoints.

### :whale2: Docker execution 

Note: *For now this installation is not covered here in detail. We will publish the corresponding images to dockerhub soon.*

1. Install `docker` and `docker-compose`.

* Docker > 18.0 [docker](https://www.docker.com/), ([install](https://docs.docker.com/install/))

Performed from the command line, this should look like this:

```bash
sudo rm /var/lib/apt/lists/*
sudo apt-get update
curl -fsSL https://get.docker.com/ | sudo sh
docker info
```

* Docker-compose ([install](https://docs.docker.com/compose/install/))

Performed from the command line, this should look like this:

```bash
curl -L https://github.com/docker/compose/releases/download/1.21.2/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
sudo docker-compose --version

2. Follow directions for building for development.

3. Run the [sbt](https://en.wikipedia.org/wiki/Sbt_(software)) docker command

```bash
sbt docker:publishLocal
```

4. Run the shell script

```bash
./run-local-docker.sh
```

For **Windows**, just grab the docker commands from the file and run directly.

### :package: Vagrant execution
This is **deprecated** but may e.g. be useful for people running Windows.

1. Download [vagrant](https://www.vagrantup.com).

2. From project directory, run 

```bash
vagrant up
```

See also

* [Vagrant setup for Windows](https://drive.google.com/file/d/1xobpv4Ew1iCN9j-M-ItU6PsfnybHUryy/view)
* [Vagrant setup for Ubuntu](https://docs.google.com/document/u/1/d/e/2PACX-1vST7vBIMxom99hKr5XyVFpM6TAs_pw-iqq403AktMWnqr3dxUFX5c0g9BWD5gU5TDPZVXKcW3HTWbVl/pub)
