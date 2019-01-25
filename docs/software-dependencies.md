## Software dependencies
### :minidisc: coreDependencies

The JVM with Java `version 8` is assured to work. Issues with earlier or later version may or may not be ironed out.

You'll get a good impression of the used external Scala tools by taking a look at the `coreDependencies`, see

* [/constellation/build.sbt](https://github.com/Constellation-Labs/constellation/blob/dev/build.sbt)

For further discussion relating to the dependiecs, see [/docs/design-choices.md](https://github.com/Constellation-Labs/constellation/blob/dev/docs/design-choices.md)

### :book: References

| Link | Package description | 
| ------------- | ------------- |
| [akka.io](https://akka.io/) resp. [wikipedia/Akka](https://en.wikipedia.org/wiki/Akka_(toolkit)) | Scala actor model library |
| [github.com/twitter/algebird](https://github.com/twitter/algebird) | Scala algebra package |
| [wikipedia/Java_Database_Connectivity](https://en.wikipedia.org/wiki/Java_Database_Connectivity) | Java database API |
| [wikipedia/LevelDB](https://en.wikipedia.org/wiki/LevelDB) | A key-value storage scheme, used for many blockchain projects |
| [wikipedia/Bouncy_Castle](https://en.wikipedia.org/wiki/Bouncy_Castle_(cryptography)) | Java cryptography implementations |
| ... | ... |


## Tools and frameworks to work with the Constellation protocol

(todo: plaintext explainations of what we use and short note on alternatives):

* [wiipedia/sbt](https://en.wikipedia.org/wiki/Sbt_(software))], Scala Build Tool
* [wikipedia/Google_Cloud_Platform](https://en.wikipedia.org/wiki/Google_Cloud_Platform), Cloud hosting
* [wikipedia/Docker](https://en.wikipedia.org/wiki/Docker_(software)), Container tool, see the 12min videos on [run](https://youtu.be/YFl2mCHdv24) and [compose](https://youtu.be/Qw9zlE3t8Ko) by J. Wright
* [grafana.com](https://grafana.com/), Analytics and monitoring platform, used with docker
* [wikipedia/Vagrant](https://en.wikipedia.org/wiki/Vagrant_(software)), VM tool
* [wikipedia/Terraform](https://en.wikipedia.org/wiki/Terraform_(software)), Infrastructure as code tool
* [circleci.com/docs/](https://circleci.com/docs/), Continuous integration 
* [wiipedia/git](https://en.wikipedia.org/wiki/Git), Distributed version control system
* ...
