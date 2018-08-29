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
  def resolveCheckpoint(dao: Data, cb: CheckpointBlock) = {


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

    // Skipping TX resolution for now as they're included in here. Fix later.
/*

    if (unresolvedParents.isEmpty) {
      true
    } else {

    }
*/

  }

}
