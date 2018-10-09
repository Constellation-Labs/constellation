package org.constellation

import akka.actor.ActorRef
import akka.testkit.TestActor
import constellation.signedObservationEdge
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.consensus.{EdgeProcessor, Validation}
import org.constellation.primitives.IncrementMetric
import org.constellation.primitives.Schema._

import scala.concurrent.Future

class CheckpointProcessorTest extends ProcessorTest {

  val bogusTxValidStatus = TransactionValidationStatus(tx, None, None)
  val ced = CheckpointEdgeData(Seq(tx.edge.signedObservationEdge.signatureBatch.hash))

  val oe = ObservationEdge(
    TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
    TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
    data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
  )

  val soe = signedObservationEdge(oe)(keyPair)
  val cb = Fixtures.createCheckpointBlock(Seq.fill(3)(tx), Seq.fill(2)(soe))(keyPair)
  val baseHash = cb.baseHash

  dbActor.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
      case DBGet(`baseHash`) =>
        sender ! Some(CheckpointCacheData(cb, true))
        TestActor.KeepRunning
      case _ =>
        sender ! None
        TestActor.KeepRunning
    }
  })

  "Incoming CheckpointBlocks" should "be signed and processed if new" in {
    EdgeProcessor.handleCheckpoint(cb, data)
    metricsManager.expectMsg(IncrementMetric("checkpointMessages"))
    assert( true )
  }

  "Previously observed CheckpointBlocks" should "indicate to metricsManager" in {
    EdgeProcessor.handleCheckpoint(cb, data, true)
    metricsManager.expectMsg(IncrementMetric("internalCheckpointMessages"))
    assert( true )
  }

  "Invalid CheckpointBlocks" should "return false" in {
    val validatedCheckpointBlock: Future[Validation.CheckpointValidationStatus] = Validation.validateCheckpointBlock(data, cb)
    validatedCheckpointBlock.map(response => assert(!response.isValid))
  }


  "CheckpointBlocks invalid by ancestry" should "return false" in {
    assert(!Validation.validByTransactionAncestors(Seq(), cb))
  }

  "CheckpointBlocks invalid by state" should "return false" in {
    val validatedCheckpointBlock = Validation.validateCheckpointBlock(data, cb)
    validatedCheckpointBlock.map(response => assert(!response.isValid))
  }

  "hashToSignedObservationEdgeCache" should "return SignedObservationEdgeCache" in {
    val res = data.hashToSignedObservationEdgeCache(cb.baseHash)
    res.map(response => assert(response.isDefined))

  }

  "hashToCheckpointCacheData" should "return CheckpointCacheData" in {
    val res = data.hashToCheckpointCacheData(cb.baseHash)
    res.map(response => assert(response.isDefined))
  }
}
