package org.constellation.schema.v2.checkpoint

import org.constellation.schema.v2.Height

case class CheckpointCacheMetadata(
  checkpointBlock: CheckpointBlockMetadata,
  children: Int = 0,
  height: Option[Height] = None
)
