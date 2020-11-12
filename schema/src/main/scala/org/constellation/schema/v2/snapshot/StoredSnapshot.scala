package org.constellation.schema.v2.snapshot

import org.constellation.schema.v2.checkpoint.CheckpointCache

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCache]) {

  def height: Long =
    checkpointCache.toList
      .maxBy(_.height.map(_.min).getOrElse(0L))
      .height
      .map(_.min)
      .getOrElse(0L)
}
