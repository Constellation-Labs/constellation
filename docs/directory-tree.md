### Annotated project directory tree

The table below provides a snapshot of the directory tree of this repository.

Note that many files are absent from here, such as:

* project files generated once you run or test the code
* shell- and sbt-scripts for building, dockerization, deploying, etc.
* configuration and setup files for build specifications and dependencies, plugins, virtual environments, deployment, unit tests, continuous integration, etc.

```scala
root
└── src
    ├── it/scala/org/constellation			<---------- continuous integration
    		ClusterDebug.scala
    		ClusterTest.scala
		DownloadChainBatch.scala
		DownloadChainSingle.scala
		Fixtures2.scala
		RestartCluster.scala
    ├── main/scala/org/constellation			<---------- protocol implementation
        ├── consensus					<─── (tbd)
		Consensus.scala
		EdgeProcessor.scala
		Resolve.scala
		TransactionProcessor.scala
		Validation.scala
        ├── crypto					<─── (tbd, as with the other folders)
        	Base58.scala
		KeyUtils.scala
		Wallet.scala (51)
        ├── p2p
		Download.scala
		PeerAPI.scala
		PeerAuth.scala
		ProbabilisticGossip.scala
		TCPServer.scala
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
	└── .     							     		<─── core classes
		API.scala
		ConstellationNode.scala
		Data.scala
		LevelDB.scala
    └── test/scala/org/constellation          		<---------- unit tests
        ├── app
		SingleAppTest.scala
       	├── cluster
		MultiNodeDAGTest.scala
        ├── consensus
		ConsensusTest.scala
        ├── p2p
		PeerToPeerTest.scala
        ├── rpc
		APIClientTest.scala
        ├── util
		KryoSerializerTest.scala
		TestNode.scala
	├── wallet
		ValidateWalletFuncTest.scala	
	└── .
		AlgebirdTest.scala
		EdgeProcessorTest.scala
		Fixtures.scala
		H2SlickTest.scala
		LevelDBTest.scala
		ShamirETHTest.scala
		SignTest.scala
		TestHelpers.scala
        	UtilityTest.scala
└── ui
```

(todo: add explainers to all folders and to the most critical files)
