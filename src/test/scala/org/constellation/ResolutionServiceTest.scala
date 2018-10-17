package org.constellation

import org.constellation.Fixtures.{createCheckpointBlock, dummyTx, getSignedObservationEdge}
import org.constellation.consensus.ResolutionService
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.APIBroadcast
import org.constellation.primitives.Schema._

class ResolutionServiceTest extends ProcessorTest {
  val bogusTxValidStatus = TransactionValidationStatus(tx, None, None)
  val soe: SignedObservationEdge = getSignedObservationEdge(tx, keyPair)
  val parentCb = Fixtures.createCheckpointBlock(Seq.fill(3)(tx), Seq.fill(2)(soe))(keyPair)
  val baseHash = soe.hash
  val bogusTx = dummyTx(data, 2L)
  val bogusSoe: SignedObservationEdge = getSignedObservationEdge(bogusTx, keyPair)
  val bogusCb: CheckpointBlock = createCheckpointBlock(Seq.fill(3)(bogusTx), Seq.fill(2)(bogusSoe))(keyPair)
  val bogusCheckpointCacheData = CheckpointCacheData(bogusCb)
//  data.dbActor.putSignedObservationEdgeCache(soe.hash, SignedObservationEdgeCache(soe, true))
//  data.dbActor.putSignedObservationEdgeCache(bogusSoe.hash, SignedObservationEdgeCache(bogusSoe, false))
//  data.dbActor.putCheckpointCacheData(parentCb.baseHash, CheckpointCacheData(parentCb, true))

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
    data.dbActor.updateSignedObservationEdgeCache(bogusSoe.hash, _.copy(resolved = false), SignedObservationEdgeCache(bogusSoe, true))
    val msg = APIBroadcast({ apiClient =>
      apiClient.get("edge/" + bogusCb.baseHash)
    }, peerSubset = bogusCb.signatures.map {
      _.toId
    }.toSet)
    val res = ResolutionService.resolveCheckpoint(mockData, CheckpointCacheData(bogusCb))
    peerManager.expectMsg(_: APIBroadcast.type )
    assert(res.exists(_.resolvedParents.length == 2))
    assert(true)
  }
}
