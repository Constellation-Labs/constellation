package org.constellation.schema.snapshot

import org.constellation.schema.checkpoint.CheckpointCache

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCache]) {

  def height: Long =
    if (checkpointCache.toList.nonEmpty) {
      checkpointCache.toList
        .maxBy(_.height.map(_.min).getOrElse(0L))
        .height
        .map(_.min)
        .getOrElse(0L)
    } else {
      0L
    }
}
