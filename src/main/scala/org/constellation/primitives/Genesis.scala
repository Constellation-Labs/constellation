package org.constellation.primitives

import org.constellation.primitives.Schema._
import constellation._
import org.constellation.LevelDB.DBPut

trait Genesis extends NodeData with Ledger with TransactionExt with BundleDataExt with EdgeDAO {

  val CoinBaseHash = "coinbase"

  def acceptGenesis(b: Bundle, tx: TransactionV1): Unit = {
    storeTransaction(tx)
    genesisBundle = Some(b)
    val md = Sheaf(b, Some(0), Map(b.extractIds.head.b58 -> 1L), Some(1000), transactionsResolved = true)
    storeBundle(md)
    maxBundleMetaData = Some(md)
    b.extractTX.foreach(acceptTransaction)
    totalNumValidBundles += 1
    val gtx = b.extractTX.head
    last100ValidBundleMetaData = Seq(md)
    gtx.txData.data.updateLedger(memPoolLedger)
  }

  def createGenesis(tx: TransactionV1): Unit = {
    downloadMode = false
    acceptGenesis(Bundle(BundleData(Seq(ParentBundleHash("coinbase"), TransactionHash(tx.hash))).signed()), tx)
  }

  /**
    * Build genesis tips and example distribution among initial nodes
    * @param ids: Initial node public keys
    * @return : Resolved edges for state update
    */
  def createGenesisAndInitialDistributionOE(ids: Set[Id]): GenesisObservation = {

    val debtAddress = makeKeyPair().address.address

    val redTXGenesisResolved = createTransactionSafeBatchOE(debtAddress, selfAddressStr, 4e9.toLong, keyPair)

    val genTXHash = redTXGenesisResolved.edge.signedObservationEdge.signatureBatch.hash

    val cb = CheckpointEdgeData(Seq(genTXHash))

    val oe = ObservationEdge(
      TypedEdgeHash(CoinBaseHash, EdgeHashType.ValidationHash),
      TypedEdgeHash(genTXHash, EdgeHashType.TransactionHash),
      data = Some(TypedEdgeHash(cb.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(oe)

    val roe = ResolvedObservationEdge(
      null.asInstanceOf[SignedObservationEdge],
      null.asInstanceOf[SignedObservationEdge],
      Some(cb)
    )

    val redGenesis = Edge(oe, soe, roe)

    val genesisCBO = CheckpointBlock(Seq(redTXGenesisResolved), CheckpointEdge(redGenesis))

    val distr = ids.toSeq.map{ id =>
      createTransactionSafeBatchOE(selfAddressStr, id.address.address, 1e6.toLong, keyPair)
    }

    val distrCB = CheckpointEdgeData(distr.map{_.edge.signedObservationEdge.signatureBatch.hash})

    val distrOE = ObservationEdge(
      TypedEdgeHash(genTXHash, EdgeHashType.TransactionHash),
      TypedEdgeHash(soe.signatureBatch.hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(distrCB.hash, EdgeHashType.CheckpointDataHash))
    )

    val distrSOE = signedObservationEdge(distrOE)

    val distrROE = ResolvedObservationEdge(soe, soe, Some(distrCB))

    val distrRED = Edge(distrOE, distrSOE, distrROE)

    val distrCBO = CheckpointBlock(distr, CheckpointEdge(distrRED))

    GenesisObservation(genesisCBO, distrCBO)

  }

  def acceptGenesisOE(go: GenesisObservation): Unit = {
    // Store hashes for the edges
    go.genesis.store(dbActor, inDAG = true, resolved = true)
    go.initialDistribution.store(dbActor, inDAG = true, resolved = true)

    // Store the balance for the genesis TX minus the distribution along with starting rep score.
    go.genesis.transactions.foreach{
      rtx =>
        dbActor ! DBPut(
          rtx.dst.hash,
          AddressCacheData(rtx.amount - go.initialDistribution.transactions.map{_.amount}.sum, Some(1000D))
        )
    }

    // Store the balance for the initial distribution addresses along with starting rep score.
    go.initialDistribution.transactions.foreach{ t =>
      dbActor ! DBPut(t.dst.hash, AddressCacheData(t.amount, Some(1000D)))
    }

    val numTX = (1 + go.initialDistribution.transactions.size).toString
    metricsManager ! UpdateMetric("validTransactions", numTX)
    metricsManager ! UpdateMetric("uniqueAddressesInLedger", numTX)

    genesisObservation = Some(go)
    validationTips = Seq(
      go.genesis.checkpoint.edge.signedObservationEdge,
      go.initialDistribution.checkpoint.edge.signedObservationEdge
    //  TypedEdgeHash(go.genesis.resolvedCB.edge.signedObservationEdge.hash, EdgeHashType.ValidationHash),
    //  TypedEdgeHash(go.initialDistribution.resolvedCB.edge.signedObservationEdge.hash, EdgeHashType.ValidationHash)
    )

    metricsManager ! UpdateMetric("activeTips", "2")

  }

}
