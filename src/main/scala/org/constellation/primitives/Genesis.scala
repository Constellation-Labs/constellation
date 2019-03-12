package org.constellation.primitives

import java.security.KeyPair

import constellation._
import org.constellation.DAO
import org.constellation.consensus.TipData
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._

object Genesis {

  final val CoinBaseHash = "coinbase"

  private final val GenesisTips = Seq(
    TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
    TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash)
  )

  def createGenesisTransaction(keyPair: KeyPair): Transaction = {
    val debtAddress = KeyUtils.makeKeyPair().address
    createTransaction(debtAddress, keyPair.getPublic.toId.address, 4e9.toLong, keyPair)
  }

  def createGenesisBlock(keyPair: KeyPair): CheckpointBlock = {
    CheckpointBlock.createCheckpointBlock(Seq(createGenesisTransaction(keyPair)), GenesisTips)(
      keyPair
    )
  }

  def start()(implicit dao: DAO): Unit = {
    // TODO: Remove initial distribution
    //val genesis = createGenesisBlock(dao.keyPair)
    val fakeIdToGenerateTips = KeyUtils.makeKeyPair().getPublic.toId
    val go = createGenesisAndInitialDistributionDirect(dao.selfAddressStr,
                                                       Set(fakeIdToGenerateTips),
                                                       dao.keyPair)
    dao.acceptGenesis(go, setAsTips = true)

  }

  // TODO: Get rid of this, need to add edge case for handling tips at beginning to avoid this
  def createDistribution(
    selfAddressStr: String,
    ids: Seq[Id],
    genesisSOE: SignedObservationEdge,
    keyPair: KeyPair
  ): CheckpointBlock = {

    val distr = ids.map { id =>
      createTransaction(selfAddressStr, id.address, 1e6.toLong, keyPair)
    }

    CheckpointBlock.createCheckpointBlock(
      distr,
      Seq(
        TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash),
        TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash)
      ),
    )(keyPair)

  }

  /**
    * Build genesis tips and example distribution among initial nodes
    *
    * @param ids: Initial node public keys
    * @return : Resolved edges for state update
    */
  def createGenesisAndInitialDistributionDirect(selfAddressStr: String,
                                                ids: Set[Id],
                                                keyPair: KeyPair): GenesisObservation = {

    val genesisCBO = createGenesisBlock(keyPair)
    val soe = genesisCBO.soe

    val distr1CBO = createDistribution(selfAddressStr, ids.toSeq, soe, keyPair)
    val distr2CBO = createDistribution(selfAddressStr, ids.toSeq, soe, keyPair)

    GenesisObservation(genesisCBO, distr1CBO, distr2CBO)
  }

}

import org.constellation.primitives.Genesis._

trait Genesis extends NodeData with EdgeDAO {

  def createGenesisAndInitialDistribution(ids: Set[Id]): GenesisObservation = {
    createGenesisAndInitialDistributionDirect(selfAddressStr, ids, keyPair)
  }

  def acceptGenesis(go: GenesisObservation, setAsTips: Boolean = false)(implicit dao: DAO): Unit = {
    // Store hashes for the edges

    go.genesis.store(
      CheckpointCacheData(Some(go.genesis), height = Some(Height(0, 0)))
    )

    go.initialDistribution.store(
      CheckpointCacheData(Some(go.initialDistribution), height = Some(Height(1, 1)))
    )

    go.initialDistribution2.store(
      CheckpointCacheData(Some(go.initialDistribution2), height = Some(Height(1, 1)))
    )

    go.genesis.storeSOE()
    go.initialDistribution.storeSOE()
    go.initialDistribution2.storeSOE()

    // Store the balance for the genesis TX minus the distribution along with starting rep score.
    go.genesis.transactions.foreach { rtx =>
      val bal = rtx.amount - (go.initialDistribution.transactions.map { _.amount }.sum * 2)
      dao.addressService.put(rtx.dst.hash,
                             AddressCacheData(bal, bal, Some(1000D), balanceByLatestSnapshot = bal))
    }

    // Store the balance for the initial distribution addresses along with starting rep score.
    go.initialDistribution.transactions.foreach { t =>
      val bal = t.amount * 2
      dao.addressService.put(t.dst.hash,
                             AddressCacheData(bal, bal, Some(1000D), balanceByLatestSnapshot = bal))
    }

    val numTX = (1 + go.initialDistribution.transactions.size * 2).toString
    //  metricsManager ! UpdateMetric("validTransactions", numTX)
    //  metricsManager ! UpdateMetric("uniqueAddressesInLedger", numTX)

    genesisObservation = Some(go)
    genesisBlock = Some(go.genesis)
    /*

    // Dumb way to set these as active tips, won't pass a double validation but no big deal.
    checkpointMemPool(go.initialDistribution.baseHash) = go.initialDistribution
    checkpointMemPool(go.initialDistribution2.baseHash) = go.initialDistribution2
    checkpointMemPoolThresholdMet(go.initialDistribution.baseHash) = go.initialDistribution -> 0
    checkpointMemPoolThresholdMet(go.initialDistribution2.baseHash) = go.initialDistribution2 -> 0
     */

    // metricsManager ! UpdateMetric("activeTips", "2")
    dao.metrics.updateMetric("genesisAccepted", "true")
    //   metricsManager ! UpdateMetric("z_genesisBlock", go.json)
    if (setAsTips) {
      dao.concurrentTipService.put(go.initialDistribution.baseHash,
                                   TipData(
                                     go.initialDistribution,
                                     0
                                   ))(dao.metrics)
      dao.concurrentTipService.put(go.initialDistribution2.baseHash,
                                   TipData(
                                     go.initialDistribution2,
                                     0
                                   ))(dao.metrics)
    }
    dao.metrics.updateMetric("genesisHash", go.genesis.soeHash)
    // println(s"accept genesis = ", go)
  }

}
