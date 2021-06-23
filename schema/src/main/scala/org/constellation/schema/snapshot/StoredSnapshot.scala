package org.constellation.schema.snapshot

import org.constellation.schema.checkpoint.{CheckpointCache, CheckpointCacheV1}

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

case class StoredSnapshotV1(snapshot: Snapshot, checkpointCache: Seq[CheckpointCacheV1]) {

  def height: Long =
    if (checkpointCache.toList.nonEmpty) {
      checkpointCache.toList
        .maxBy(_.height.get.min)
        .height
        .get
        .min
    } else {
      0L
    }
}
