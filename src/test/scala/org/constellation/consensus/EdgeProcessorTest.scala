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

  var transactions: Seq[Transaction] = _
  var tips: Seq[SignedObservationEdge] = _
  var soed: SignedObservationEdge = _
  val edgeProcessor = TestProbe()

  implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)
  implicit val kp: KeyPair = KeyUtils.makeKeyPair()

  data.edgeProcessor = edgeProcessor.ref
  data.metricsManager = metricsManager.ref
  data.keyPair = keyPair

  val CoinBaseHash = "coinbase"

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

  soed = signedObservationEdge(obe)

  tips = Seq(soed, soed)

  transactions = Seq(dummyTx(data), dummyTx(data))

  "The handleConflictingCheckpoint method" should "handle an incoming checkpoint with more signatures then the cached data" in {

    val existingCheckpointBlock = createCheckpointBlock(transactions, tips)

    val newKeyPair = KeyUtils.makeKeyPair()

    val conflictingCheckpointBlock = existingCheckpointBlock.plus(newKeyPair)

    var childCheckpoint = createCheckpointBlock(transactions, Seq(soed, SignedObservationEdge(SignatureBatch(conflictingCheckpointBlock.soeHash, Seq()))))

    val newKeyPair2 = KeyUtils.makeKeyPair()

    childCheckpoint = childCheckpoint.plus(newKeyPair2)

    val childCheckpointCache = CheckpointCacheData(childCheckpoint, soeHash = childCheckpoint.soeHash)

    val cccd = CheckpointCacheData(conflictingCheckpointBlock, soeHash = conflictingCheckpointBlock.soeHash, children = Set(childCheckpoint.soeHash))

    val cpcd = CheckpointCacheData(existingCheckpointBlock, soeHash = existingCheckpointBlock.soeHash)

    val cpHash = cpcd.checkpointBlock.soeHash
    val ccHash = conflictingCheckpointBlock.soeHash
    val baseHash = conflictingCheckpointBlock.baseHash

    val childBaseHash = childCheckpoint.baseHash
    val childSoeHash = childCheckpoint.soeHash

    data.dbActor.putSignedObservationEdgeCache(ccHash, SignedObservationEdgeCache(SignedObservationEdge(SignatureBatch(baseHash, Seq()))))

    data.dbActor.putSignedObservationEdgeCache(childSoeHash, SignedObservationEdgeCache(SignedObservationEdge(SignatureBatch(childBaseHash, Seq()))))

    data.dbActor.putCheckpointCacheData(baseHash, cccd)

    data.dbActor.putCheckpointCacheData(childBaseHash, childCheckpointCache)

    edgeProcessor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case LookupEdge(`childSoeHash`) =>
          val edge = EdgeProcessor.lookupEdge(data, childSoeHash)
          sender ! edge
          edge
          TestActor.KeepRunning
      }
    })

    val mostValidCheckpointBlock = EdgeProcessor.handleConflictingCheckpoint(cpcd, conflictingCheckpointBlock, data)

    val storedCheckpointCacheData = data.dbActor.getCheckpointCacheData(mostValidCheckpointBlock.checkpointBlock.baseHash).get

    data.dbActor.delete(ccHash)
    data.dbActor.delete(childSoeHash)
    data.dbActor.delete(baseHash)
    data.dbActor.delete(childBaseHash)
    data.dbActor.delete(mostValidCheckpointBlock.checkpointBlock.baseHash)

    assert(conflictingCheckpointBlock.soeHash == storedCheckpointCacheData.soeHash)

    assert(storedCheckpointCacheData.checkpointBlock == conflictingCheckpointBlock)

    assert(mostValidCheckpointBlock == cccd)
  }

  "The handleConflictingCheckpoint method" should "handle an incoming checkpoint with less signatures then the cached data" in {

    var existingCheckpointBlock = createCheckpointBlock(transactions, tips)

    val newKeyPair = KeyUtils.makeKeyPair()

    val newKeyPair2 = KeyUtils.makeKeyPair()
    val newKeyPair3 = KeyUtils.makeKeyPair()

    val conflictingCheckpointBlock = existingCheckpointBlock.plus(newKeyPair)

    existingCheckpointBlock = existingCheckpointBlock.plus(newKeyPair2).plus(newKeyPair3)

    var childCheckpoint = createCheckpointBlock(transactions, Seq(soed, SignedObservationEdge(SignatureBatch(conflictingCheckpointBlock.soeHash, Seq()))))

    childCheckpoint = childCheckpoint.plus(newKeyPair2)

    val childCheckpointCache = CheckpointCacheData(childCheckpoint, soeHash = childCheckpoint.soeHash)

    val cccd = CheckpointCacheData(conflictingCheckpointBlock, soeHash = conflictingCheckpointBlock.soeHash, children = Set(childCheckpoint.soeHash))

    val cpcd = CheckpointCacheData(existingCheckpointBlock, soeHash = existingCheckpointBlock.soeHash)

    val cpHash = cpcd.checkpointBlock.soeHash
    val ccHash = conflictingCheckpointBlock.soeHash
    val baseHash = conflictingCheckpointBlock.baseHash

    val childBaseHash = childCheckpoint.baseHash
    val childSoeHash = childCheckpoint.soeHash

    data.dbActor.putSignedObservationEdgeCache(ccHash, SignedObservationEdgeCache(SignedObservationEdge(SignatureBatch(baseHash, Seq()))))

    data.dbActor.putSignedObservationEdgeCache(childSoeHash, SignedObservationEdgeCache(SignedObservationEdge(SignatureBatch(childBaseHash, Seq()))))

    data.dbActor.putCheckpointCacheData(baseHash, cccd)

    data.dbActor.putCheckpointCacheData(childBaseHash, childCheckpointCache)

    edgeProcessor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case LookupEdge(`childSoeHash`) =>
          val edge = EdgeProcessor.lookupEdge(data, childSoeHash)
          sender ! edge
          edge
          TestActor.KeepRunning
      }
    })

    val mostValidCheckpointBlock = EdgeProcessor.handleConflictingCheckpoint(cpcd, conflictingCheckpointBlock, data)

    val storedCheckpointCacheData = data.dbActor.getCheckpointCacheData(mostValidCheckpointBlock.checkpointBlock.baseHash).get

    data.dbActor.delete(ccHash)
    data.dbActor.delete(childSoeHash)
    data.dbActor.delete(baseHash)
    data.dbActor.delete(childBaseHash)
    data.dbActor.delete(mostValidCheckpointBlock.checkpointBlock.baseHash)

    assert(cpcd.checkpointBlock.soeHash == storedCheckpointCacheData.soeHash)

    assert(storedCheckpointCacheData.checkpointBlock == cpcd.checkpointBlock)

    assert(mostValidCheckpointBlock == cpcd)
  }

}
