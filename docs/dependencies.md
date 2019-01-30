## Software dependencies
### :minidisc: Core dependencies
The Java Virtual Machine (JVM) coming with 
[Java](https://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
`version 8` is assured to work. E.g. download the `jdk-8u202` executable. 
Issues with earlier or later version may or may not be ironed out.

For Scala, see [www.scala-lang.org/download/](https://www.scala-lang.org/download/).

### Protocol
You'll get a good impression of the used external Scala modules used by taking a look at the `coreDependencies`, 
see [/constellation/build.sbt](https://github.com/Constellation-Labs/constellation/blob/dev/build.sbt).

#### :book: References
More explicitly, here some of the main ones

| Link | Package description |
| ------------- | ------------- |
| [akka.io](https://akka.io/) resp. [wikipedia/Akka](https://en.wikipedia.org/wiki/Akka_(toolkit)) | Scala actor model library |
| [github.com/twitter/algebird](https://github.com/twitter/algebird) | Scala algebra package |
| [wikipedia/Java_Database_Connectivity](https://en.wikipedia.org/wiki/Java_Database_Connectivity) | Java database API |
| [wikipedia/LevelDB](https://en.wikipedia.org/wiki/LevelDB) | A key-value storage scheme, used for many blockchain projects |
| [wikipedia/Bouncy_Castle](https://en.wikipedia.org/wiki/Bouncy_Castle_(cryptography)) | Java cryptography implementations |
| ... | ... |

## Tools and frameworks
Additional software used to work with the Constellation protocol, whether as de facto default or optional.

* [wikipedia/sbt](https://en.wikipedia.org/wiki/Sbt_(software))] ... Scala Build Tool, see also [scaladoc1](https://docs.scala-lang.org/style/scaladoc.html), [scaladoc2](https://docs.scala-lang.org/overviews/scaladoc/for-library-authors.html) and [scaladoc3](https://alvinalexander.com/scala/how-to-generate-scala-documentation-scaladoc-command-examples) for how to document code and write resp. generate the `sbt doc` library html.
* [wikipedia/Google_Cloud_Platform](https://en.wikipedia.org/wiki/Google_Cloud_Platform) ... Cloud hosting - see 11min video on [how to set up a VM](https://youtu.be/chk2rRjSn5o) by sentdex
* [wikipedia/PuTTY](https://en.wikipedia.org/wiki/PuTTY) ... SSH and telnet client ([download](https://www.putty.org/)), can be used to establish connection with GCP
* [grafana.com](https://grafana.com/) ... Analytics and monitoring platform, used with docker
* [mkdocs.org](https://www.mkdocs.org/) ... Project documentation generator - see also [/dev/readme-gh-pages.md](https://github.com/Constellation-Labs/constellation/blob/dev/readme-gh-pages.md)
* [wikipedia/Vagrant](https://en.wikipedia.org/wiki/Vagrant_(software)) ... Virtual Machine (VM) tool
* [wikipedia/Terraform](https://en.wikipedia.org/wiki/Terraform_(software)) ... Infrastructure as code tool
* [circleci.com/docs/](https://circleci.com/docs/) ... Continuous integration 
* [wikipedia/git](https://en.wikipedia.org/wiki/Git) ... Distributed version control system
* ...
* [wikipedia/Docker](https://en.wikipedia.org/wiki/Docker_(software)) ... Container tool - see the 12min videos on [docker-run](https://youtu.be/YFl2mCHdv24) reps. [docker-compose](https://youtu.be/Qw9zlE3t8Ko) by Jake Wright

### Docker
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
