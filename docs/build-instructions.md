## Build instructions
### Prerequisites
Depending on the way you want to run the node, you may only need to install Java 8.x, the latest Scala and a build tool.
See [/docs/dependencies](https://github.com/Constellation-Labs/constellation/blob/dev/docs/dependencies.md).

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

1. Install [docker](https://en.wikipedia.org/wiki/Docker_(software)) and docker-compose. See the Docker section in [/docs/dependencies.md](https://github.com/Constellation-Labs/constellation/blob/dev/docs/dependencies).

2. Follow directions for building for development. 

(TODO) Note: *For now this installation is not covered here in detail. We will publish the corresponding images to dockerhub soon.*

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
