package org.constellation.primitives

import java.security.KeyPair

import org.constellation.primitives.Schema._
import constellation._
import org.constellation.LevelDB.DBPut

import scala.collection.concurrent.TrieMap


object Genesis {

  val CoinBaseHash = "coinbase"

  def createDistribution(
                          selfAddressStr: String, ids: Seq[Id], genesisSOE: SignedObservationEdge, keyPair: KeyPair
                        ): CheckpointBlock = {

    val distr = ids.map{ id =>
      createTransaction(selfAddressStr, id.address.address, 1e6.toLong, keyPair)
    }

    val distrCB = CheckpointEdgeData(distr.map{_.edge.signedObservationEdge.signatureBatch.hash})

    val distrOE = ObservationEdge(
      TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(distrCB.hash, EdgeHashType.CheckpointDataHash))
    )

    val distrSOE = signedObservationEdge(distrOE)(keyPair)

    val distrROE = ResolvedObservationEdge(genesisSOE, genesisSOE, Some(distrCB))

    val distrRED = Edge(distrOE, distrSOE, distrROE)

    val distrCBO = CheckpointBlock(distr, CheckpointEdge(distrRED))

    distrCBO
  }

  /**
    * Build genesis tips and example distribution among initial nodes
    * @param ids: Initial node public keys
    * @return : Resolved edges for state update
    */
  def createGenesisAndInitialDistributionDirect(selfAddressStr: String, ids: Set[Id], keyPair: KeyPair): GenesisObservation = {

    val debtAddress = makeKeyPair().address.address

    val redTXGenesisResolved = createTransaction(debtAddress, selfAddressStr, 4e9.toLong, keyPair)

    val genTXHash = redTXGenesisResolved.edge.signedObservationEdge.signatureBatch.hash

    val cb = CheckpointEdgeData(Seq(genTXHash))

    val oe = ObservationEdge(
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(cb.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(oe)(keyPair)

    val roe = ResolvedObservationEdge(
      null.asInstanceOf[SignedObservationEdge],
      null.asInstanceOf[SignedObservationEdge],
      Some(cb)
    )

    val redGenesis = Edge(oe, soe, roe)

    val genesisCBO = CheckpointBlock(Seq(redTXGenesisResolved), CheckpointEdge(redGenesis))

    val distr1CBO = createDistribution(selfAddressStr, ids.toSeq, soe, keyPair)
    val distr2CBO = createDistribution(selfAddressStr, ids.toSeq, soe, keyPair)

    GenesisObservation(genesisCBO, distr1CBO, distr2CBO)
  }

}

import Genesis._

trait Genesis extends NodeData with Ledger with BundleDataExt with EdgeDAO {

  def createGenesisAndInitialDistribution(ids: Set[Id]): GenesisObservation = {
    createGenesisAndInitialDistributionDirect(selfAddressStr, ids, keyPair)
  }

  def acceptGenesis(go: GenesisObservation): Unit = {
    // Store hashes for the edges
    go.genesis.store(dbActor, CheckpointCacheData(go.genesis, inDAG = true, resolved = true), resolved = true)
    go.initialDistribution.store(dbActor, CheckpointCacheData(go.initialDistribution, inDAG = true, resolved = true), resolved = true)
    go.initialDistribution2.store(dbActor, CheckpointCacheData(go.initialDistribution2, inDAG = true, resolved = true), resolved = true)

    // Store the balance for the genesis TX minus the distribution along with starting rep score.
    go.genesis.transactions.foreach{
      rtx =>
        val bal = rtx.amount - (go.initialDistribution.transactions.map {_.amount}.sum * 2)
        dbActor ! DBPut(
          rtx.dst.hash,
          AddressCacheData(bal, bal, Some(1000D))
        )
    }

    // Store the balance for the initial distribution addresses along with starting rep score.
    go.initialDistribution.transactions.foreach{ t =>
      val bal = t.amount * 2
      dbActor ! DBPut(t.dst.hash, AddressCacheData(bal, bal, Some(1000D)))
    }

    val numTX = (1 + go.initialDistribution.transactions.size * 2).toString
  //  metricsManager ! UpdateMetric("validTransactions", numTX)
  //  metricsManager ! UpdateMetric("uniqueAddressesInLedger", numTX)

    genesisObservation = Some(go)

    // Dumb way to set these as active tips, won't pass a double validation but no big deal.
    checkpointMemPool(go.initialDistribution.baseHash) = go.initialDistribution
    checkpointMemPool(go.initialDistribution2.baseHash) = go.initialDistribution2
    checkpointMemPoolThresholdMet(go.initialDistribution.baseHash) = go.initialDistribution -> 0
    checkpointMemPoolThresholdMet(go.initialDistribution2.baseHash) = go.initialDistribution2 -> 0

   // metricsManager ! UpdateMetric("activeTips", "2")
    metricsManager ! UpdateMetric("genesisAccepted", "true")
 //   metricsManager ! UpdateMetric("z_genesisBlock", go.json)

    println(s"accept genesis = ", go)
  }

}
