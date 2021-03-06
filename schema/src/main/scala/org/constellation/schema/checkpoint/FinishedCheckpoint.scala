package org.constellation.schema.checkpoint

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.Id

case class FinishedCheckpoint(checkpointCacheData: CheckpointCache, facilitators: Set[Id])

object FinishedCheckpoint {
  implicit val ord: Ordering[FinishedCheckpoint] =
    Ordering.by[FinishedCheckpoint, CheckpointCache](_.checkpointCacheData)

  implicit val finishedCheckpointEncoder: Encoder[FinishedCheckpoint] = deriveEncoder
  implicit val finishedCheckpointDecoder: Decoder[FinishedCheckpoint] = deriveDecoder
}
