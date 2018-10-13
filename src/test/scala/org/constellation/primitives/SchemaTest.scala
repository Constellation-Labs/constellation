package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import akka.util.Timeout
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.scalatest.FlatSpec
import org.constellation.Fixtures._
import constellation._
import org.constellation.ProcessorTest
import org.constellation.consensus.EdgeProcessor
import org.constellation.consensus.EdgeProcessor.LookupEdge

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

    val checkpointEdge = CheckpointEdge(
      Edge(obe, soed, ResolvedObservationEdge(tips.head, tips(1), Some(CheckpointEdgeData(Seq()))))
    )

    val tipHash = soed.hash

    data.dbActor.putSignedObservationEdgeCache(soed.hash, SignedObservationEdgeCache(soed))

    data.dbActor.putCheckpointCacheData(soed.baseHash, CheckpointCacheData(CheckpointBlock(Seq(), checkpointEdge), soeHash = "test"))

    val transactions = Seq(dummyTx(data), dummyTx(data))

    val checkpointBlock = createCheckpointBlock(transactions, tips)

    val checkpointCacheData = CheckpointCacheData(checkpointBlock, soeHash = checkpointBlock.soeHash)

    val edgeProcessor = TestProbe()

    edgeProcessor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case LookupEdge(`tipHash`) =>
          val edge = EdgeProcessor.lookupEdge(data, tipHash)
          sender ! edge
          edge
          TestActor.KeepRunning
      }
    })

    val initialStoredParentCheckpointCache = data.dbActor.getCheckpointCacheData(soed.baseHash).get

    assert(initialStoredParentCheckpointCache.children.isEmpty)

    val updates = checkpointCacheData.updateParentsChildRefs(edgeProcessor.ref, data.dbActor)

    val afterStoredParentCheckpointCache = data.dbActor.getCheckpointCacheData(soed.baseHash).get

    assert(afterStoredParentCheckpointCache.children.size == 1)

    assert(afterStoredParentCheckpointCache.children.head == checkpointCacheData.checkpointBlock.soeHash)
  }

}
