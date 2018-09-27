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

object Resolve {
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  /**
    * Find out if both parents are resolved. If not, mark parents unresolved.
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): Future[Boolean] = {

    // TODO: temp
    dao.confirmedCheckpoints(cb.baseHash) = cb

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

//
//  // WIP
//  def resolveCheckpoint(dao: Data, cb: CheckpointBlock)(implicit ec: ExecutionContext): Future[Boolean] = {
//
//    // Step 1 - Find out if both parents are resolved.
//
//    val parentCache = Future.sequence(cb.checkpoint.edge.parentHashes
//      .map { h => dao.hashToSignedObservationEdgeCache(h).map{h -> _} }
//    )
//
//    val isResolved = parentCache.map{ cache =>
//      val parentsResolved = cache.forall(_._2.exists(_.resolved))
//      val missingParents = cache.filter{case (h, c) => c.isEmpty && !dao.resolveNotifierCallbacks.contains(h)}
//
//      missingParents.foreach { case (hash, oECache) =>
//        oECache.foreach(dao.markParentsUnresolved(hash, cb))
//      }
//      // TODO: Right now not storing CB in DB until it's been resolved, when that changes
//      // (due to status info requirements) may ? need to have a check here to reflect that.
//      parentsResolved
//    }
//    isResolved
//  }