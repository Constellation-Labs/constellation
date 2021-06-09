package org.constellation.schema.checkpoint

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Height

case class CheckpointCache(
  checkpointBlock: CheckpointBlock,
  children: Int = 0,
  height: Height = Height(0L, 0L)
)

object CheckpointCache {
  implicit val checkpointCacheOrdering: Ordering[CheckpointCache] =
    Ordering.by[CheckpointCache, Long](_.height.min)

  implicit val checkpointCacheEncoder: Encoder[CheckpointCache] = deriveEncoder
  implicit val checkpointCacheDecoder: Decoder[CheckpointCache] = deriveDecoder
}
