## Build instructions
### :computer: Building for development
#### On Linux and Mac
1. Check out the repository 
```haskell
git clone git@github.com:Constellation-Labs/constellation.git
```
2. From root directory `constellation`, run 
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

#### Docker execution 

Note: *For now this installation is not covered here in detail.*

**Install docker locally**
1. Set up [docker](https://www.docker.com/).
2. Follow directions for building for development.
3. Run
```haskell
sbt docker:publishLocal
```
4. Run
```haskell
./run-local-docker.sh
```

We will publish the corresponding images to dockerhub soon.

For Windows, just grab the docker ```cmd``` from the file and run directly.

#### Vagrant execution
This is **deprecated** but may be useful for people running Windows, etc.:
1. Download [vagrant](https://www.vagrantup.com).
2. Run 
```haskell
vagrant up
```
from project directory. See also
	* [Vagrant setup for Windows](https://drive.google.com/file/d/1xobpv4Ew1iCN9j-M-ItU6PsfnybHUryy/view)
	* [Vagrant setup for Ubuntu](https://docs.google.com/document/u/1/d/e/2PACX-1vST7vBIMxom99hKr5XyVFpM6TAs_pw-iqq403AktMWnqr3dxUFX5c0g9BWD5gU5TDPZVXKcW3HTWbVl/pub)
