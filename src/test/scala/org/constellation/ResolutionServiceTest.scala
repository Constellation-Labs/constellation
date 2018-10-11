package org.constellation

import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActor, TestProbe}
import constellation.signedObservationEdge
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.ResolutionService
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, PeerData}
import org.constellation.util.APIClient
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContextExecutor

class ResolutionServiceTest extends ProcessorTest {
  val bogusTxValidStatus = TransactionValidationStatus(tx, None, None)

  dbActor.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
      case DBGet(`baseHash`) =>
        sender ! Some(CheckpointCacheData(parentCb, true))
        TestActor.KeepRunning
      case _ =>
        sender ! Some(CheckpointCacheData(parentCb, true))
        TestActor.KeepRunning
    }
  })

  def getSignedObservationEdge(trx: Transaction) = {
    val ced = CheckpointEdgeData(Seq(tx.edge.signedObservationEdge.signatureBatch.hash))
    val oe = ObservationEdge(
      TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
    )
    val soe: SignedObservationEdge = signedObservationEdge(oe)(keyPair)
    soe
  }

  val soe = getSignedObservationEdge(tx)
  val parentCb = Fixtures.createCheckpointBlock(Seq.fill(3)(tx), Seq.fill(2)(soe))(keyPair)
  val baseHash = soe.hash

  val bogusTx = dummyTx(data, 2L)
  val bogusSoe = getSignedObservationEdge(bogusTx)
  val bogusCb = Fixtures.createCheckpointBlock(Seq.fill(3)(bogusTx), Seq.fill(2)(bogusSoe))(keyPair)

  "CheckpointBlocks with resolved parents" should "have isAhead = false " in {
    val (resolvedParents, unresolvedParents): (Seq[(String, Option[SignedObservationEdgeCache])], Seq[(String, Option[SignedObservationEdgeCache])]) = ResolutionService.partitionByParentsResolved(mockData, parentCb)
      assert(unresolvedParents.isEmpty)
  }

  "CheckpointBlocks with unresolved parents" should "have isAhead = true" in {
    val (resolvedParents, unresolvedParents) = ResolutionService.partitionByParentsResolved(mockData, bogusCb)
      assert(resolvedParents.isEmpty)
  }

  "CheckpointBlocks that are invalid " should "have isAhead = false " in {
    val res: ResolutionService.ResolutionStatus = ResolutionService.resolveCheckpoint(mockData, parentCb)
      assert(!data.snapshotRelativeTips.contains(parentCb))
  }

  "CheckpointBlocks that are ahead" should "query the signers" in {
    val msg = APIBroadcast({ apiClient =>
      apiClient.get("edge/" + bogusCb.baseHash)
    }, peerSubset = bogusCb.signatures.map {
      _.toId
    }.toSet)
    val res = ResolutionService.resolveCheckpoint(mockData, bogusCb)
    peerManager.expectMsg(msg)
    assert(res.unresolvedParents.nonEmpty)
  }
}
