### Annotated project directory tree

The table below provides a snapshot of the directory tree of this repository.

Note that many files are absent from here, such as:
	 * project files generated once you run or test the code
	 * shell- and sbt-scripts for building, dockerization, deploying, etc.
	 * configuration and setup files for build specifications and dependencies, plugins, virtual environments, deployment, unit tests, continuous integration, etc.

```scala
constellation (root)
└── src
    ├── it/scala/org/constellation             <─── continuous integration
			ClusterDebug.scala
    		ClusterTest.scala
			DownloadChainBatch.scala
			DownloadChainSingle.scala
			Fixtures2.scala
			RestartCluster.scala
	├── main/scala/org
		package.scala
    ├── main/scala/org/constellation           <─── protocol implementation
        ├── consensus					       <─── (tbd)
            Consensus.scala
            EdgeProcessor.scala
			Resolve.scala
			TransactionProcessor.scala
			Validation.scala
        ├── crypto		       				   <─── (tbd)
            Base58.scala
            KeyUtils.scala
            Wallet.scala (51)
        ├── p2p
            Download.scala       		 	
            PeerAPI.scala       			
            PeerAuth.scala       			
            ProbabilisticGossip.scala  	
            TCPServer.scala       			
            └── UDPActor.scala
        ├── primitives
            ActiveDAGManager.scala
			BundleDataExt.scala
			CellManager.scala
			EdgeDAO.scala
			Genesis.scala
			Ledger.scala
			MemPoolManager.scala
			MetricsExt.scala
			MetricsManager.scala
			NodeData.scala
			PeerInfo.scala
			PeerManager.scala
			Reputation.scala
			Schema.scala
			TransactionValidation.scala
        ├── serializer
			ConstellationKryoRegistrar.scala
			KryoSerializer.scala
			PubKeyKryoSerializer.scala
		├── transactions
			TransactionManager.scala
        ├── util
            APIClient.scala
            CommonEndpoints.scala
            Metrics.scala
            ServeUI.scala
			Signed.scala
            LevelDB.scala
			Simulation.scala
		└── .
            API.scala
            ConstellationNode.scala
            Data.scala
            LevelDB.scala
    └── test/scala/org/constellation          <─── unit tests
        ├── app
		 	└── SingleAppTest.scala
       	├── cluster
			└── MultiNodeDAGTest.scala
        ├── consensus
		 	└── ConsensusTest.scala
        ├── p2p
			PeerToPeerTest.scala
		 	└── UDPTest.scala
        ├── rpc
			└── APIClientTest.scala
        ├── util
		 	KryoSerializerTest.scala
			└── TestNode.scala
		├── wallet
			└── ValidateWalletFuncTest.scala	
		└── .
			AlgebirdTest.scala 	
			EdgeProcessorTest.scala
			Fixtures.scala
			H2SlickTest.scala
			LevelDBTest.scala
			ShamirETHTest.scala
			SignTest.scala
			TestHelpers.scala
        	└── UtilityTest.scala## Contents
The docs provide technical information to understand, build and run the code base
developed in this repository. The following are covered:
    * annotated project folder tree
    * archetecture diagrams
    * build instructions
    * code base archetecture documentation
    * project dependencies
    * recommended tools
	* discussion of desgin choices
	* development process

## Wait, there's more...
### Wiki
Note that there is the github Wiki of this repository, which itself holds
information that puts the Constellation approach in context to other distributed
consensus protocols:
    * [/constellation/wiki](https://github.com/Constellation-Labs/constellation/wiki)

There you find a 
[FAQ](https://github.com/Constellation-Labs/constellation/wiki/FAQ), 
[comparisons](https://github.com/Constellation-Labs/constellation/wiki/Comparisons-to-other-protocols)
against other projects, 
[recommended reading](https://github.com/Constellation-Labs/constellation/wiki/Recommended-Reading), 
etc.

### Resource list
A comprehensive resource list which also holds material for developing on top of
the protcol can be found in the awesome-constellatio repository:
    * [/awesome-constellation](https://github.com/Constellation-Labs/awesome-constellation)
└── ui
```

