package org.constellation.primitives

import org.constellation.primitives.Schema._
import constellation._

trait Genesis extends NodeData with Ledger with TransactionExt with BundleDataExt {

  val CoinBaseHash = "coinbase"

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

    val redGenesis = ResolvedEdgeData(oe, soe, roe)

    val genesisCBO = ResolvedCBObservation(Seq(redTXGenesisResolved), ResolvedCB(redGenesis))

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

    val distrRED = ResolvedEdgeData(distrOE, distrSOE, distrROE)

    val distrCBO = ResolvedCBObservation(distr, ResolvedCB(distrRED))

    GenesisObservation(genesisCBO, distrCBO)

  }

}
