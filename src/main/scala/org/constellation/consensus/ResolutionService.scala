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
                               checkpointCacheData: CheckpointCacheData,
                               resolvedParents: Seq[(String, SignedObservationEdgeCache)],
                               unresolvedParents: Seq[(String, Option[SignedObservationEdgeCache])]
                             )

  /**
    *
    * @param dao
    * @param checkpointCache
    * @param missingParents
    */
  def processUnresolvedParents(dao: Data, checkpointCache: CheckpointCacheData, missingParents: Seq[String]) =
    missingParents.foreach(dao.markParentsUnresolved(_, checkpointCache.checkpointBlock))

  /**
    *
    * @param dao
    * @param unresolvedChildren
    */
  def reprocessUnresolvedChildren(dao: Data, unresolvedChildren: Seq[CheckpointBlock])
                                 (implicit executionContext: ExecutionContext) = unresolvedChildren.foreach { unresolvedChild =>
    EdgeProcessor.handleCheckpoint(unresolvedChild, dao, true) //Todo log exception add retry
  }

  /**
    * If this newly resolved checkpoint block was a previously unresolved parent, reprocess the child
    *
    * @param dao
    * @param children
    * @param executionContext
    * @return
    */
  def isUnresolved(dao: Data, children: Seq[CheckpointBlock])(implicit executionContext: ExecutionContext): Seq[CheckpointBlock] =
    children.filter { child =>
      dao.dbActor.getSignedObservationEdgeCache(child.baseHash).exists(!_.resolved)
    }

  /**
    *
    * @param dao
    * @param cb
    * @return
    */
  def partitionByParentsResolved(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext):
  (Seq[(String, Option[SignedObservationEdgeCache])], Seq[(String, Option[SignedObservationEdgeCache])]) = {
    val parentCache = cb.checkpoint.edge.parentHashes.map { hash => (hash, dao.dbActor.getSignedObservationEdgeCache(hash)) }
    parentCache.partition { case (_, queryResult) => queryResult.exists(_.resolved) }
  }

  /**
    * Update view to account for newly observed cb and store cb.
    *
    * @param dao
    * @param executionContext
    * @return
    */
  def resolveCheckpoint(dao: Data, checkpointCache: CheckpointCacheData)(implicit executionContext: ExecutionContext): Option[ResolutionStatus] = {
    val children = dao.resolveNotifierCallbacks.get(checkpointCache.checkpointBlock.baseHash)
    val unresolvedChildren = children.map(isUnresolved(dao, _))
    if (!checkpointCache.resolved) {
      val (resolvedParents, unresolvedParents) = partitionByParentsResolved(dao, checkpointCache.checkpointBlock)
      val isResolved = unresolvedParents.isEmpty
      val checkpointCacheData = CheckpointCacheData(checkpointCache.checkpointBlock, resolved = isResolved, children = children.getOrElse(Seq()).map(_.baseHash).toSet) //Todo change type on children, this is gross
      if (!isResolved) processUnresolvedParents(dao, checkpointCache, unresolvedParents.map{ case (h, _) => h })

      checkpointCache.checkpointBlock.storeResolution(dao.dbActor, checkpointCacheData)
      Some(ResolutionStatus(checkpointCacheData, resolvedParents.flatMap { case (h, soec) => soec.map((h, _)) }, unresolvedParents))
    } else {
      unresolvedChildren.foreach(reprocessUnresolvedChildren(dao, _))
      None
    }
  }
}