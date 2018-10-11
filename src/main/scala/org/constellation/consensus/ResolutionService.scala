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
                               cb: CheckpointCacheData,
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
    *
    * @param dao
    * @param unresolvedChildren
    */
  def reprocessUnresolvedChildren(dao: Data, unresolvedChildren: Seq[CheckpointBlock])(implicit executionContext: ExecutionContext) = unresolvedChildren.foreach { unresolvedChild =>
    EdgeProcessor.handleCheckpoint(unresolvedChild, dao, true) //Todo log exception add retry
  }

  /**
    *
    * @param dao
    * @param cb
    */
  def reprocessUnresolvedParents(dao: Data, cb: CheckpointBlock): (String, Option[SignedObservationEdgeCache]) => Unit = {
    case (missingParentHash: String, queryResult: Option[SignedObservationEdgeCache]) =>
      if (queryResult.isEmpty) downloadAncestry(dao, cb)
      dao.markParentsUnresolved(missingParentHash, cb)
  }

  /**
    * If this newly resolved checkpoint block was a previously unresolved parent, reprocess the child
    *
    * @param dao
    * @param children
    * @param executionContext
    * @return
    */
  def isUnresolved(dao: Data, children: Seq[CheckpointBlock])(implicit executionContext: ExecutionContext): Seq[CheckpointBlock] = {
    children.filter { child =>
      dao.dbActor.getSignedObservationEdgeCache(child.baseHash).exists(!_.resolved)
    }
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
    val (unresolvedParents, resolvedParents) = parentCache.partition { case (hash, queryResult) => queryResult.exists(_.resolved) }
    (unresolvedParents, resolvedParents)
  }

  /**
    * Update view to account for newly observed cb and store cb.
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): ResolutionStatus = {
    val (unresolvedParents, resolvedParents) = partitionByParentsResolved(dao, cb)
    val isResolved = unresolvedParents.isEmpty
    val children = dao.resolveNotifierCallbacks.get(cb.baseHash)
    val unresolvedChildren = children.map(isUnresolved(dao, _))
    val checkpointCacheData = CheckpointCacheData(cb, resolved = isResolved, children = children.getOrElse(Seq()).map(_.baseHash).toSet)//Todo change type on children, this is gross

    if (!isResolved) unresolvedParents.foreach { case (h, qR) => reprocessUnresolvedParents(dao, cb)(h, qR) }
    unresolvedChildren.foreach(reprocessUnresolvedChildren(dao, _))
    cb.storeResolution(dao.dbActor, checkpointCacheData)

    ResolutionStatus(checkpointCacheData, unresolvedParents, resolvedParents)
  }
}