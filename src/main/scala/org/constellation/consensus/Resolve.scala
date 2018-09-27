package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import org.constellation.Data
import org.constellation.LevelDB.DBGet
import org.constellation.primitives.Schema.{CheckpointBlock, CheckpointCacheData, SignedObservationEdgeCache}
import constellation.EasyFutureBlock
import org.constellation.primitives.APIBroadcast

import scala.concurrent.{ExecutionContext, Future}

import scala.concurrent.{ExecutionContext, Future}

object Resolve {
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
  // TODO: Need to include signatories ABOVE this checkpoint block later in the case
  // of signature decay.
  def attemptResolveIndividual(dao: Data, cb: CheckpointBlock, h: String) = {
    cb.signatures.map{_.toId}

    dao.peerManager ? APIBroadcast({
      apiClient =>
        apiClient.get("edge/" + h)
    })
  }

  /**
    * Find out if both parents are resolved. If not, mark parents unresolved.
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    val parentCache = Future.sequence(cb.checkpoint.edge.parentHashes
      .map { h => dao.hashToSignedObservationEdgeCache(h).map{h -> _} }
    )

    parentCache.map { cache: Seq[(String, Option[SignedObservationEdgeCache])] =>
        cache.map { case (parentHash, oECache: Option[SignedObservationEdgeCache]) =>
          val isMissing = parentHash.isEmpty && !dao.resolveNotifierCallbacks.contains(parentHash)
          val isResolved = oECache.exists(_.resolved)
          if (isMissing) dao.markParentsUnresolved(parentHash, cb)
          isResolved
        }.forall(_ == true)
    }
  }
}