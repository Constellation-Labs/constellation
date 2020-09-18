package org.constellation.schema.checkpoint

import org.constellation.schema.Height

case class CheckpointCacheMetadata(
  checkpointBlock: CheckpointBlockMetadata,
  children: Int = 0,
  height: Option[Height] = None
)
