# constellation

[![Join the chat at https://gitter.im/constellation_protocol/Lobby](https://badges.gitter.im/constellation_protocol/Lobby.svg)](https://gitter.im/constellation_protocol/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A blockchain microservice operating system

# Building for development
#### Instructions tested on Mac and Linux (may work elsewhere)
1. Check out repository (git clone git@github.com:Constellation-Labs/constellation.git)
2. From constellation directory, run ./build.sh -- optionally ./build.sh seedhost:port (to connect to other host)
3. Interact with app via the api (by default http://localhost:9000, look at API.scala for endpoints)

# Docker execution (assuming you have docker already set up)
##### Install docker locally -- for now not covering that here.
##### We will publish docker images to dockerhub soon(ish).
1. Follow directions for building for development (other than step 4).
2. sbt docker:publishLocal
3. ./run-local-docker.sh (for windows, just grab the docker cmd from the file and run directly.)

# Api Docs
#### WIP -- we intend to use Swagger or similar to publish comprehensive API docs.

# Vagrant execution
### Deprecated -- but may be useful for people running Windows, etc.
1. download vagrant https://www.vagrantup.com
2. run ```vagrant up``` from project directory.

# Community guides
### Thanks to those who provided these!
#### Instructions are assuming vagrant, so are being deprecated,but we appreciate the community support.
1. Vagrant setup for windows: https://drive.google.com/file/d/1xobpv4Ew1iCN9j-M-ItU6PsfnybHUryy/view
2. Vagrant setup for ubuntu: https://docs.google.com/document/u/1/d/e/2PACX-1vST7vBIMxom99hKr5XyVFpM6TAs_pw-iqq403AktMWnqr3dxUFX5c0g9BWD5gU5TDPZVXKcW3HTWbVl/pub

