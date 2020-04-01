package org.constellation.domain.observation

import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import cats.implicits._
import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.consensus.Consensus.RoundId
import org.constellation.primitives.CheckpointBlock

sealed trait ObservationEvent {
  val kind: String
}

case class CheckpointBlockWithMissingParents(checkpointBaseHash: String) extends ObservationEvent {
  val kind = "CheckpointBlockWithMissingParents"
}
case class CheckpointBlockWithMissingSoe(checkpointBaseHash: String) extends ObservationEvent {
  val kind = "CheckpointBlockWithMissingSoe"
}
case class RequestTimeoutOnConsensus(roundId: RoundId) extends ObservationEvent {
  val kind = "RequestTimeoutOnConsensus"
}
case class RequestTimeoutOnResolving(hashes: List[String]) extends ObservationEvent {
  val kind = "RequestTimeoutOnResolving"
}
case class CheckpointBlockInvalid(checkpointBaseHash: String, reason: ValidationResult[CheckpointBlock])
    extends ObservationEvent {
  val kind = "CheckpointBlockInvalid"
}

object ObservationEvent {
  implicit val encodeEvent: Encoder[ObservationEvent] = Encoder.instance {
    case a @ CheckpointBlockWithMissingParents(_) => a.asJson
    case a @ CheckpointBlockWithMissingSoe(_)     => a.asJson
    case a @ RequestTimeoutOnConsensus(_)         => a.asJson
    case a @ RequestTimeoutOnResolving(_)         => a.asJson
    case a @ CheckpointBlockInvalid(_, _)         => a.asJson
  }

  implicit val decodeEvent: Decoder[ObservationEvent] =
    List[Decoder[ObservationEvent]](
      Decoder[CheckpointBlockWithMissingParents].widen,
      Decoder[CheckpointBlockWithMissingSoe].widen,
      Decoder[RequestTimeoutOnConsensus].widen,
      Decoder[RequestTimeoutOnResolving].widen,
      Decoder[CheckpointBlockInvalid].widen
    ).reduceLeft(_.or(_))
}
