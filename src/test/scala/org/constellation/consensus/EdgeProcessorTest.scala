package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActor, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.crypto.KeyUtils
import org.constellation.{Data, LevelDBActor, ProcessorTest}
import org.constellation.Fixtures._
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.EdgeProcessor.LookupEdge
import org.constellation.primitives.Schema._
import org.constellation.util.{SignatureBatch, Signed}
import org.scalatest.{FlatSpec, FlatSpecLike}
import org.constellation.util.SignHelp._

class EdgeProcessorTest extends ProcessorTest {

  "The handleConflictingCheckpoint method" should "handle conflicting checkpointBlocks" in {

    val metricsManager = TestProbe()

    val dao = data

    implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

    val dbActor = TestProbe()

    val edgeProcessor = TestProbe()

    dao.edgeProcessor = edgeProcessor.ref

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

    val childCheckpoint = createCheckpointBlock(transactions, Seq(soed, SignedObservationEdge(SignatureBatch(conflictingCheckpointBlock.soeHash, Seq()))))

    val childCheckpointCache = CheckpointCacheData(childCheckpoint, soeHash = childCheckpoint.soeHash)

    val cccd = CheckpointCacheData(conflictingCheckpointBlock, soeHash = conflictingCheckpointBlock.soeHash, children = Set(childCheckpoint.soeHash))

    val cpcd = CheckpointCacheData(existingCheckpointBlock, soeHash = existingCheckpointBlock.soeHash)

    val cpHash = cpcd.checkpointBlock.soeHash
    val ccHash = conflictingCheckpointBlock.soeHash
    val baseHash = conflictingCheckpointBlock.baseHash

    val childBaseHash = childCheckpoint.baseHash
    val childSoeHash = childCheckpoint.soeHash

    dbActor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case DBGet(`cpHash`) =>
          sender ! Some(cpcd.checkpointBlock.resolvedOE)
          TestActor.KeepRunning
        case DBGet(`ccHash`) =>
          sender ! Some(SignedObservationEdgeCache(SignedObservationEdge(SignatureBatch(baseHash, Seq()))))
          TestActor.KeepRunning
        case DBGet(`baseHash`) =>
          sender ! Some(cccd)
          TestActor.KeepRunning
        case DBGet(`childSoeHash`) =>
          sender ! Some(SignedObservationEdgeCache(SignedObservationEdge(SignatureBatch(childBaseHash, Seq()))))
          TestActor.KeepRunning
        case DBGet(`childBaseHash`) =>
          sender ! Some(childCheckpointCache)
          TestActor.KeepRunning
        case _ =>
          sender ! None
          TestActor.KeepRunning
      }
    })

    edgeProcessor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case LookupEdge(`childSoeHash`) =>
          val edge = EdgeProcessor.lookupEdge(dao, childSoeHash)
          sender ! edge
          edge
          TestActor.KeepRunning
      }
    })

    val mostValidCheckpointBlock = EdgeProcessor.handleConflictingCheckpoint(cpcd, conflictingCheckpointBlock, dao)

    assert(mostValidCheckpointBlock == cccd)
  }

}
