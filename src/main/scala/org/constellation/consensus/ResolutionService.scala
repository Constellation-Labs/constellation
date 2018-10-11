package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.util.Timeout
import org.constellation.Data
import org.constellation.primitives.{APIBroadcast, Schema}
import org.constellation.primitives.Schema.{CheckpointBlock, CheckpointCacheData, SignedObservationEdgeCache}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

object ResolutionService {
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  case class ResolutionStatus(
                               cb: CheckpointBlock,
                               resolvedParents: Seq[(String, Option[SignedObservationEdgeCache])],
                               unresolvedParents: Seq[(String, Option[SignedObservationEdgeCache])]
                             )

  /**
    * TODO: Need to include signatories ABOVE this checkpoint block later in the case of signature decay. Add to SignedObservationEdgeCache
    *
    * @param dao
    * @param cb
    * @return
    */
  def downloadAncestry(dao: Data, cb: CheckpointBlock) = {
    val observers: Set[Schema.Id] = cb.signatures.map {
      _.toId
    }.toSet
    dao.queryMissingResolutionData(cb.baseHash, observers)
  }

  /**
    * If this newly resolved checkpoint block was a previously unresolved parent, reprocess the child
    *
    * @param dao
    * @param cb
    */
  def reprocessUnresolvedChildren(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext) = {
    val children = dao.resolveNotifierCallbacks(cb.baseHash)
    val unresolvedChildren = children.map { child =>
      val isResolved = dao.hashToSignedObservationEdgeCache(child.baseHash).filter(soeCache => soeCache.exists(!_.resolved))
      isResolved.map(resolutionStatus => child)
    }
    unresolvedChildren.foreach { unresolvedChildrenFut =>
      unresolvedChildrenFut.map { unresolvedChild =>
        EdgeProcessor.handleCheckpoint(unresolvedChild, dao, true)
      }//Todo log exception add retry
    }
  }

  /**
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def partitionByParentsResolved(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext) = {
    val parentCache = Future.sequence(cb.checkpoint.edge.parentHashes.map { hash =>
        dao.hashToSignedObservationEdgeCache(hash).map { hash -> _ }
      }
    )
    parentCache.map { query: Seq[(String, Option[SignedObservationEdgeCache])] =>
      val (unresolvedParents, resolvedParents) = query.partition { case (hash, queryResult) => queryResult.exists(_.resolved) }
      (unresolvedParents, resolvedParents)
    }
  }

  /**
    * Update view to account for newly observed cb and store cb.
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): Future[ResolutionStatus] = {
    partitionByParentsResolved(dao, cb).map { case (unresolvedParents, resolvedParents) =>
      if (unresolvedParents.isEmpty){
        cb.markResolved(dao.dbActor, CheckpointCacheData(cb, inDAG = true, resolved = true, soeHash = cb.soeHash))
      }
      else {
        unresolvedParents.foreach {
          case (missingParentHash, queryResult) =>
            if (queryResult.isEmpty) downloadAncestry(dao, cb)
            dao.markParentsUnresolved(missingParentHash, cb)
        }
      }
      reprocessUnresolvedChildren(dao, cb)
      ResolutionStatus(cb, unresolvedParents, resolvedParents)
    }
  }
}