package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import org.constellation.Data
import org.constellation.LevelDB.DBGet
import org.constellation.primitives.Schema.{CheckpointBlock, CheckpointCacheData, SignedObservationEdgeCache}
import constellation.EasyFutureBlock

object Resolve {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // WIP
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock): Boolean = {

    // Step 1 - Find out if both parents are resolved.

    // TODO: Change to Future.sequence
    val parentCache = cb.checkpoint.edge.parentHashes.map{ h =>
      h -> (dao.dbActor ? DBGet(h)).mapTo[Option[SignedObservationEdgeCache]].get()
    }

    val parentsResolved = parentCache.forall(_._2.exists(_.resolved))

    // Later on need to add transaction resolving, they're all already here though (sent with the request)
    // which adds redundancy but simplifies this step.

    // Break out if we're done here.
    if (parentsResolved) {
      return true
    }

    // TODO: temp
    dao.confirmedCheckpoints(cb.baseHash) = cb

    false

    /*
    val missingParents = parentCache.filter(_._2.isEmpty)

    missingParents

    // (dao.dbActor ? DBGet(cb.hash)).mapTo[Option[CheckpointCacheData]]

    val unresolvedParents = cb.checkpoint.edge.parents.foreach{
      h =>
        if (dao.validationMemPool.contains(h.hash)) {
          true
        } else {
          val cache = (dao.dbActor ? DBGet(h.hash)).mapTo[Option[SignedObservationEdgeCache]].get()
          if (cache.isEmpty) {
            if (dao.resolveNotifierCallbacks.contains(h.hash)) {
              val notifiers = dao.resolveNotifierCallbacks(h.hash)
              if (!notifiers.contains(cb.hash)) {
                notifiers
              }
            }
          }
        //  .nonEmpty
        }
    }
*/
    // Skipping TX resolution for now as they're included in here. Fix later.
/*

    if (unresolvedParents.isEmpty) {
      true
    } else {

    }
*/

  }

}
