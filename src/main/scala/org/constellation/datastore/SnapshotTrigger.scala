package org.constellation.datastore

import scala.concurrent.Future

import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.util.Periodic

/** Documentation. */
class SnapshotTrigger(periodSeconds: Int = 1)(implicit dao: DAO)
  extends Periodic("SnapshotTrigger", periodSeconds) {

  /** Documentation. */
  override def trigger(): Future[Any] = {
    Snapshot.triggerSnapshot(round)
  }

}
