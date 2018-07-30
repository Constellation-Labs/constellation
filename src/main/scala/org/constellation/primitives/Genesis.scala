package org.constellation.primitives

import org.constellation.primitives.Schema._
import constellation._

trait Genesis extends NodeData with Ledger with TransactionExt with BundleDataExt {

  def acceptGenesis(b: Bundle, tx: Transaction): Unit = {
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

  def createGenesis(tx: Transaction): Unit = {
    downloadMode = false
    acceptGenesis(Bundle(BundleData(Seq(ParentBundleHash("coinbase"), TransactionHash(tx.hash))).signed()), tx)
  }

  def createGenesisAndInitialDistributionOE(ids: Set[Id]): GenesisObservation = {
    val debtAddress = makeKeyPair().address.address
    val ResolvedTX(tx, txData) = createTransactionSafeBatchOE(debtAddress, selfAddressStr, 4e9.toLong, keyPair)
    val cb = CheckpointBlock(Set(tx.hash))
    val oe = ObservationEdge("coinbase", cb.hash)
    val soe = signedObservationEdge(oe)

    val genesisTip = ResolvedTipObservation(Set(ResolvedTX(tx, txData)), cb, oe, soe)

    val distr = ids.map{ id =>
      createTransactionSafeBatchOE(selfAddressStr, id.address.address, 1e6.toLong, keyPair)
    }

    val distrCB = CheckpointBlock(distr.map{_.tx.hash})
    val distrOE = ObservationEdge(soe.hash, distrCB.hash)
    val distrSOE = signedObservationEdge(distrOE)

    val distrTip = ResolvedTipObservation(
      distr, distrCB, distrOE, distrSOE
    )

    GenesisObservation(genesisTip, distrTip)

  }

}
