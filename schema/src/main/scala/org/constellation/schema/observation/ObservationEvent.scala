package org.constellation.schema.observation

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.consensus.RoundId

sealed trait ObservationEvent

case class CheckpointBlockWithMissingParents(checkpointBaseHash: String) extends ObservationEvent
case class CheckpointBlockWithMissingSoe(checkpointBaseHash: String) extends ObservationEvent
case class RequestTimeoutOnConsensus(roundId: RoundId) extends ObservationEvent
case class RequestTimeoutOnResolving(hashes: List[String]) extends ObservationEvent
case class CheckpointBlockInvalid(checkpointBaseHash: String, reason: String) extends ObservationEvent

object ObservationEvent {
  implicit val encodeEvent: Encoder[ObservationEvent] = deriveEncoder
  implicit val decodeEvent: Decoder[ObservationEvent] = deriveDecoder
}
