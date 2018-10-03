package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.constellation.Data
import org.constellation.primitives.Schema
import org.constellation.primitives.Schema.{CheckpointBlock, SignedObservationEdgeCache}

import scala.concurrent.{ExecutionContext, Future}

object ResolutionService {
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  case class ResolutionStatus(isAhead: Boolean, greatestAncestor: Option[CheckpointBlock] = None)

  /**
    * Find greatest common ancestor shared between newly observed cb and active tip surface. If not equal to last snapshot,
    * cb is a branch (potential fork)
    *
    * @param dao
    * @param cb
    * @return
    */
  def greatestCommonAncestor(dao: Data, cb: CheckpointBlock): Option[CheckpointBlock] = None

  /**
    * Index newly observed cb.
    *
    * @param dao
    * @param gca
    */
  def storeBranch(dao: Data, gca: CheckpointBlock, cb: CheckpointBlock): Unit = {
    dao.branches(cb.baseHash) = gca

  }


  // TODO: Need to include signatories ABOVE this checkpoint block later in the case of signature decay.
  def downloadAncestry(dao: Data, cb: CheckpointBlock) = {
    val observers: Set[Schema.Id] = cb.signatures.map {
      _.toId
    }.toSet //Todo flatMap over db queries to impl above.
    dao.queryMissingResolutionData(cb.baseHash, observers)
  }

  /**
    *
    * @param parentHash
    * @param oECache
    * @param dao
    * @param cb
    * @return
    */
  def parentResolution(parentHash: String, oECache: Option[SignedObservationEdgeCache])(dao: Data, cb: CheckpointBlock) = {
    val isMissing = parentHash.isEmpty | dao.resolveNotifierCallbacks.contains(parentHash)
    val isResolved = oECache.exists(_.resolved)
    if (isMissing) dao.markParentsUnresolved(parentHash, cb)
    isResolved
  }

  /**
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def findRoot(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext) = {
    val parentCache = Future.sequence(cb.checkpoint.edge.parentHashes
      .map { h => dao.hashToSignedObservationEdgeCache(h).map {
        h -> _
      }
      }
    )
    parentCache.map { query: Seq[(String, Option[SignedObservationEdgeCache])] =>
      val parentsResolved = query.map { case (hash, queryResult) => parentResolution(hash, queryResult)(dao, cb) }.forall(_ == true)
      if (!parentsResolved) ResolutionStatus(isAhead = true, greatestAncestor = greatestCommonAncestor(dao, cb))
      else ResolutionStatus(isAhead = false, greatestAncestor = greatestCommonAncestor(dao, cb))
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
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): Future[ResolutionStatus] =
    findRoot(dao, cb).map {
      case rs@ResolutionStatus(false, gcaOpt) =>
        if (gcaOpt.contains(dao.snapshot.getOrElse(None))) dao.addToSnapshotRelativeTips(cb)
        else gcaOpt.foreach(storeBranch(dao, _, cb))
        rs
      case rs@ResolutionStatus(true, None) =>
        downloadAncestry(dao, cb)
        rs
    }
}