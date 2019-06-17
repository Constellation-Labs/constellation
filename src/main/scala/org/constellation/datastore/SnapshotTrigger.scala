package org.constellation.datastore

import scala.concurrent.Future
import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.util.Periodic

import scala.util.Try

class SnapshotTrigger(periodSeconds: Int = 5)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("SnapshotTrigger", periodSeconds) {

  override def trigger(): Future[Try[Unit]] = {
    Snapshot.triggerSnapshot(round)
  }

}
