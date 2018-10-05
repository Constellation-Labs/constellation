package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.constellation.LevelDB.DBGet
import org.constellation.primitives.Schema.{CheckpointBlock, CheckpointCacheData, SignedObservationEdgeCache}
import constellation._
import org.constellation.Data
import akka.pattern.ask
import org.constellation.consensus.EdgeProcessor

import scala.concurrent.Future

trait EdgeExt extends NodeData with Ledger with MetricsExt with PeerInfo with EdgeDAO {

  def queryMissingResolutionData(h: String, signers: Set[Schema.Id]) = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    peerManager ? APIBroadcast({
      apiClient =>
        apiClient.get("edge/" + h)
    }, peerSubset = signers)
  }

  def addToSnapshotRelativeTips(cb: CheckpointBlock) = snapshotRelativeTips + cb

  def markParentsUnresolved(missingParentHash: String, cb: CheckpointBlock) =
    resolveNotifierCallbacks.get(missingParentHash) match {
        case Some(cbs) if !cbs.contains(cb) =>
            resolveNotifierCallbacks(missingParentHash) :+= cb
        case None =>
          queryMissingResolutionData(missingParentHash, cb.signatures.map{_.toId}.toSet)
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