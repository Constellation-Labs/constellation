package org.constellation.schema.snapshot

import org.constellation.schema.checkpoint.CheckpointCache

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCache]) {

  def height: Long =
    if (checkpointCache.toList.nonEmpty) {
      checkpointCache.toList
        .maxBy(_.height.min)
        .height
        .min
    } else {
      0L
    }
}
