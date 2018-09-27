package org.constellation.consensus

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import org.constellation.crypto.KeyUtils
import org.constellation.Data
import org.constellation.Fixtures._
import org.constellation.primitives.Schema._
import org.constellation.util.Signed
import org.scalatest.{FlatSpec, FlatSpecLike}
import org.constellation.util.SignHelp._

class EdgeProcessorTest extends TestKit(ActorSystem("EdgeProcessorTest")) with FlatSpecLike {

  "The handleDuplicateTransactions method" should "handle conflicting checkpointBlocks" in {

    val metricsManager = TestProbe()

    val dao = new Data()

    dao.metricsManager = metricsManager.ref

    implicit val keyPair: KeyPair = KeyUtils.makeKeyPair()

    dao.keyPair = keyPair

    val CoinBaseHash = "coinbase"

    val debtAddress = KeyUtils.makeKeyPair().getPublic
    val selfAddress = KeyUtils.makeKeyPair().getPublic

    val dumTx = dummyTx(dao)

    val genTXHash = dumTx.edge.signedObservationEdge.signatureBatch.hash

    val cb = CheckpointEdgeData(Seq(genTXHash))

    val obe = ObservationEdge(
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(cb.hash, EdgeHashType.CheckpointDataHash))
    )

    val soed = signedObservationEdge(obe)

    val tips = Seq(soed, soed)

    val transactions = Seq(dummyTx(dao), dummyTx(dao))

    val existingCheckpointBlock = createCheckpointBlock(transactions, tips)

    val newKeyPair = KeyUtils.makeKeyPair()

    val conflictingCheckpointBlock = existingCheckpointBlock.plus(newKeyPair)

    val cpcd = CheckpointCacheData(existingCheckpointBlock)

    val mostValidCheckpointBlock = EdgeProcessor.handleDuplicateTransactions(cpcd, conflictingCheckpointBlock, dao)

    assert(mostValidCheckpointBlock == conflictingCheckpointBlock)
  }

}
