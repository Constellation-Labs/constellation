package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.constellation.LevelDB.DBGet
import org.constellation.primitives.Schema.{CheckpointBlock, CheckpointCacheData, SignedObservationEdgeCache}
import constellation._
import org.constellation.Data
import akka.pattern.ask

import scala.concurrent.Future

trait EdgeExt extends NodeData with Ledger with MetricsExt with PeerInfo with EdgeDAO {

  // TODO: Need to include signatories ABOVE this checkpoint block later in the case of signature decay.
  def attemptResolveIndividual(cb: CheckpointBlock, h: String) = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    cb.signatures.map{_.toId}

    peerManager ? APIBroadcast({
      apiClient =>
        apiClient.get("edge/" + h)
    })
  }

  def markParentsUnresolved(missingParentHash: String, cb: CheckpointBlock) =
    resolveNotifierCallbacks.get(missingParentHash) match {
        case Some(cbs) =>
          if (!cbs.contains(cb)) {
            resolveNotifierCallbacks(missingParentHash) :+= cb
          }
        case None =>
          attemptResolveIndividual(cb, missingParentHash)
          resolveNotifierCallbacks(missingParentHash) = Seq(cb)
      }

  def hashToSignedObservationEdgeCache(hash: String): Future[Option[SignedObservationEdgeCache]] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    (dbActor ? DBGet(hash)).mapTo[Option[SignedObservationEdgeCache]]
  }

  def hashToCheckpointCacheData(hash: String) = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    (dbActor ? DBGet(hash)).mapTo[Option[CheckpointCacheData]]
  }
}