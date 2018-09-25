## Constellation
Decentralized Application Integration Platform.

This repository is the reference implementation of our 
[DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) 
protocol using the 
[Scala](https://www.scala-lang.org/) 
programming language.

Visit the docs for an explainer of the protocol archetecture or an extended overview of the 
[project structure](https://github.com/Constellation-Labs/constellation/docs/directory-tree/)

* [/constellation/docs](https://github.com/Constellation-Labs/constellation/docs/)

Note that the `master` branch might be behind the `dev` branch by a few weeks.

```scala
constellation/src
├── it/scala/org/constellation           <─── continuous integration
      ClusterDebug.scala
├── main/scala/org/constellation         <─── protocol implementation
    ├── consensus
          Consensus.scala
          EdgeProcessor.scala
    ├── crypto
          Wallet.scala
    ├── p2p
    ├── primitives
          TransactionValidation.scala
    ├── serializer
    └── util
          API.scala
          ConstellationNode.scala
          LevelDB.scala
└── test/scala/org/constellation          <─── unit tests
    ├── consensus
    └── rpc
```

### Development
#### :computer: Build instructions
For details on the build process, as well as pointers for docker, vagrant and deployment, see

* [/docs/build-instructions](https://github.com/Constellation-Labs/constellation/blob/developer/nikolaj/add-docs/docs/build-instructions.md)

#### :rotating_light: Troubleshooting
For issues and bug reports, see

* [/wiki/Contribution-guidelines](https://github.com/Constellation-Labs/constellation/wiki/Contribution-guidelines)

#### :green_book: API Docs
We intend to use **Swagger** or similar to publish comprehensive API docs.

#### :books: Resources
For developer tools and documentation beyond the 
[/docs](https://github.com/Constellation-Labs/constellation/tree/developer/nikolaj/add-docs/docs)
and the 
[repository wiki](https://github.com/Constellation-Labs/constellation/wiki), 
as well as :two_hearts: community constributions, contribution guidelines and so on, you can find links to the community outlets in the 
[**resource list**](https://github.com/Constellation-Labs/awesome-constellation). 
