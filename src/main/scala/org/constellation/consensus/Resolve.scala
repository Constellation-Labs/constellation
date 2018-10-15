package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import org.constellation.DAO
import org.constellation.LevelDB.DBGet
import org.constellation.Data
import org.constellation.primitives.APIBroadcast
import org.constellation.primitives.Schema.{CheckpointBlock, SignedObservationEdgeCache}
import org.constellation.primitives.Schema.CheckpointBlock

import scala.concurrent.{ExecutionContext, Future}

object Resolve {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // TODO: Need to include signatories ABOVE this checkpoint block later in the case
  // of signature decay.
  def attemptResolveIndividual(dao: DAO, cb: CheckpointBlock, h: String) = {
    cb.signatures.map {
      _.toId
    }

    dao.peerManager ? APIBroadcast({
      apiClient =>
        apiClient.get("edge/" + h)
    })
  }

  // WIP
  def resolveCheckpoint(dao: DAO, cb: CheckpointBlock)(implicit ec: ExecutionContext): Future[Boolean] = {

    // Step 1 - Find out if both parents are resolved.
    Future {

      val cache = cb.checkpoint.edge.parentHashes.map { h => h -> dao.dbActor.getSignedObservationEdgeCache(h) }

      val parentsResolved = cache.forall(_._2.exists(_.resolved))

      val missingParents = cache.filter { case (h, c) => c.isEmpty && !dao.resolveNotifierCallbacks.contains(h) }

      missingParents.foreach {
        case (h, c) =>
          dao.resolveNotifierCallbacks.get(h) match {
            case Some(cbs) =>
              if (!cbs.contains(cb)) {
                dao.resolveNotifierCallbacks(h) :+= cb
              }
            case None =>
              attemptResolveIndividual(dao, cb, h)
              dao.resolveNotifierCallbacks(h) = Seq(cb)
          }
      }

      // TODO: Right now not storing CB in DB until it's been resolved, when that changes
      // (due to status info requirements) may ? need to have a check here to reflect that.

      parentsResolved
    }


  }

}
