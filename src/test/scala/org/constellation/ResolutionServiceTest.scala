package org.constellation

import org.constellation.Fixtures.{createCheckpointBlock, dummyTx, getSignedObservationEdge}
import org.constellation.consensus.ResolutionService
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.APIBroadcast
import org.constellation.primitives.Schema._
import org.scalatest.FlatSpec

class ResolutionServiceTest extends FlatSpec with ProcessorTest {
  val bogusTxValidStatus = TransactionValidationStatus(tx, None, None)
  val soe: SignedObservationEdge = getSignedObservationEdge(tx, keyPair)
  val parentCb = Fixtures.createCheckpointBlock(Seq.fill(3)(tx), Seq.fill(2)(soe))(keyPair)
  val baseHash = soe.hash
  val bogusTx = dummyTx(data, 2L)
  val bogusSoe: SignedObservationEdge = getSignedObservationEdge(bogusTx, keyPair)
  val bogusCb: CheckpointBlock = createCheckpointBlock(Seq.fill(3)(bogusTx), Seq.fill(2)(bogusSoe))(keyPair)
  val bogusCheckpointCacheData = CheckpointCacheData(bogusCb)
  (data.dbActor.getSignedObservationEdgeCache _).when(srcHash).returns(Some(SignedObservationEdgeCache(soe, true)))
  (data.dbActor.getSignedObservationEdgeCache _).when(bogusSoe.hash).returns(Some(SignedObservationEdgeCache(bogusSoe, false)))

  "CheckpointBlocks that are resolved" should "return None" in {
    val res = ResolutionService.resolveCheckpoint(mockData, CheckpointCacheData(bogusCb, resolved = true))
    assert(res.isEmpty)
  }

  "CheckpointBlocks with no resolved parents" should "not have resolvedParents" in {
    val res = ResolutionService.resolveCheckpoint(mockData, bogusCheckpointCacheData)
    assert(res.exists(_.resolvedParents.isEmpty))
    assert(res.exists(_.unresolvedParents.length == 2))
  }

  "CheckpointBlocks that are ahead" should "query the signers" in {
    val msg = APIBroadcast({ apiClient =>
      apiClient.get("edge/" + bogusCb.baseHash)
    }, peerSubset = bogusCb.signatures.map {
      _.toId
    }.toSet)
    val res = ResolutionService.resolveCheckpoint(mockData, bogusCheckpointCacheData)
    peerManager.expectMsg(_: APIBroadcast.type )
    assert(res.exists(_.resolvedParents.isEmpty))
  }
}
