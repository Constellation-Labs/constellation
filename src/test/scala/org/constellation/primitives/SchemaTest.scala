package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.testkit.TestProbe
import akka.util.Timeout
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.scalatest.FlatSpec
import org.constellation.Fixtures._
import constellation._
import org.constellation.ProcessorTest

class SchemaTest extends ProcessorTest {

  implicit val kp: KeyPair = KeyUtils.makeKeyPair()

  "CheckpointCacheData updateParentsChildRefs" should "update correct references" in {

    val CoinBaseHash = "coinbase"

    implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

    val debtAddress = KeyUtils.makeKeyPair().getPublic
    val selfAddress = KeyUtils.makeKeyPair().getPublic

    val dumTx = dummyTx(data)

    val genTXHash = dumTx.edge.signedObservationEdge.signatureBatch.hash

    val cb = CheckpointEdgeData(Seq(genTXHash))

    val obe = ObservationEdge(
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(cb.hash, EdgeHashType.CheckpointDataHash))
    )

    val soed = signedObservationEdge(obe)

    val tips = Seq(soed, soed)

    val transactions = Seq(dummyTx(data), dummyTx(data))

    val checkpointBlock = createCheckpointBlock(transactions, tips)

    val checkpointCacheData = CheckpointCacheData(checkpointBlock, soeHash = checkpointBlock.soeHash)

    val edgeProcessor = TestProbe()

    val updates = checkpointCacheData.updateParentsChildRefs(edgeProcessor.ref, data.dbActor)

    assert(updates == Seq())
  }

}
