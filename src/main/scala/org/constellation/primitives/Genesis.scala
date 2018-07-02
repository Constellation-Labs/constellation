package org.constellation.primitives

import org.constellation.primitives.Schema._
import constellation._

trait Genesis extends NodeData with Ledger with TransactionExt with BundleDataExt {

  def acceptGenesis(b: Bundle): Unit = {
    genesisBundle = b
    val md = BundleMetaData(b, Some(0), Map(id.b58 -> 1L), Some(1000), transactionsResolved = true)
    storeBundle(md)
    maxBundleMetaData = md
    genesisBundle.extractTX.foreach(acceptTransaction)
    totalNumValidBundles += 1
    val gtx = b.extractTX.head
    gtx.txData.data.updateLedger(memPoolLedger)
  }

  def createGenesis(tx: TX): Unit = {
    db.put(tx)
    acceptGenesis(Bundle(BundleData(Seq(ParentBundleHash("coinbase"), TransactionHash(tx.hash))).signed()))
    downloadMode = false
  }

}
