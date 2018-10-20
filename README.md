## Constellation
This repository is the reference implementation of our **Decentralized Application Integration Platform** using the 
[Scala](https://www.scala-lang.org/)
programming language. We build a horizontally scalable 
[DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) 
protocol that you can build upon and interface with using common software standards.

### :books: Resources
Visit the repository
[**wiki**](https://github.com/Constellation-Labs/constellation/wiki) 
for developer tools and documentation and consult the 
[**/docs**](https://github.com/Constellation-Labs/constellation/docs/).
for an explainer of the protocol archetecture or an extended overview of the 
[project structure](https://github.com/Constellation-Labs/constellation/docs/directory-tree/).

```scala
src
├── it/scala/org/constellation           <─── continuous integration
├── main/scala/org/constellation         <─── protocol implementation
    ├── consensus
          Consensus.scala
    ├── crypto
          KeyUtils.scala
    ├── p2p
    ├── primitives
    ├── serializer
    └── util
          API.scala
          ConstellationNode.scala
└── test/scala/org/constellation          <─── unit tests
```

### :computer: Build instructions
For details on the build process, as well as pointers for docker, vagrant and deployment, see [/docs/build-instructions](https://github.com/Constellation-Labs/constellation/blob/developer/nikolaj/add-docs/docs/build-instructions.md).

### :green_book: API Docs
We intend to use **Swagger** to publish comprehensive API docs.

### :rotating_light: Troubleshooting
For issues and bug reports, see [wiki/Contribution-guidelines](https://github.com/Constellation-Labs/constellation/wiki/Contribution-guidelines). 

There you also find general pointers toward the development process. Note that the `master` branch might be behind the `dev` branch by a few weeks.

### :two_hearts: Community
For questions and contributions, can find links to the community outlets and more in the 
[**resource list**](https://github.com/Constellation-Labs/awesome-constellation). 
Our knowledge- and news-outlet is the 
[**Orion**](https://orion.constellationlabs.io/) 
Discourse forum. 

Join our [Discord](https://discordapp.com/invite/KMSmXbV) server for a chat. 

  <a href="https://discordapp.com/invite/KMSmXbV">
	  <img src="https://img.shields.io/badge/chat-discord-brightgreen.svg"/>
  </a>
