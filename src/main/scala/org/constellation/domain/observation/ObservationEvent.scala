package org.constellation.domain.observation

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import cats.syntax.all._
import org.constellation.consensus.Consensus.RoundId

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
