package org.constellation.primitives

import org.constellation.primitives.Schema._
import constellation._
import org.constellation.LevelDB.DBPut

trait Genesis extends NodeData with EdgeDAO {

  val CoinBaseHash = "coinbase"

  /**
    * Build genesis tips and example distribution among initial nodes
    * @param ids: Initial node public keys
    * @return : Resolved edges for state update
    */
  def createGenesisAndInitialDistribution(ids: Set[Id]): GenesisObservation = {

    val debtAddress = makeKeyPair().address.address

    val redTXGenesisResolved = createTransaction(debtAddress, selfAddressStr, 4e9.toLong, keyPair)

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
      createTransaction(selfAddressStr, id.address.address, 1e6.toLong, keyPair)
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

  def acceptGenesis(go: GenesisObservation): Unit = {
    // Store hashes for the edges
    go.genesis.store(dbActor, inDAG = true)
    go.initialDistribution.store(dbActor, inDAG = true)

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

    println(s"accept genesis = ", go)
  }

}
