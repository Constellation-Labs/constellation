package org.constellation.schema.v2.checkpoint

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.Height

case class CheckpointCache(
  checkpointBlock: CheckpointBlock,
  children: Int = 0,
  height: Option[Height] = None // TODO: Check if Option if needed
)

object CheckpointCache {
  implicit val checkpointCacheOrdering: Ordering[CheckpointCache] =
    Ordering.by[CheckpointCache, Long](_.height.fold(0L)(_.min))

  implicit val checkpointCacheEncoder: Encoder[CheckpointCache] = deriveEncoder
  implicit val checkpointCacheDecoder: Decoder[CheckpointCache] = deriveDecoder
}
