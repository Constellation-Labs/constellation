package org.constellation.domain.healthcheck

import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import org.constellation.schema.Id

sealed trait HealthCheckStatus

object HealthCheckStatus {
  implicit val encodeHealthCheckStatus: Encoder[HealthCheckStatus] = Encoder.instance {
    case a: MissingProposalHealthCheckStatus => a.asJson
    case a: PeerPingHealthCheckStatus        => a.asJson
  }

  implicit val decodeHealthCheckStatus: Decoder[HealthCheckStatus] = List[Decoder[HealthCheckStatus]](
    Decoder[MissingProposalHealthCheckStatus].widen,
    Decoder[PeerPingHealthCheckStatus].widen
  ).reduceLeft(_.or(_))

  sealed trait PeerPingHealthCheckStatus extends HealthCheckStatus

  object PeerPingHealthCheckStatus {
    implicit val encodePeerHealthCheckStatus: Encoder[PeerPingHealthCheckStatus] = deriveEncoder
    implicit val decodePeerHealthCheckStatus: Decoder[PeerPingHealthCheckStatus] = deriveDecoder
  }

  case class PeerAvailable(id: Id) extends PeerPingHealthCheckStatus

  case class PeerUnresponsive(id: Id) extends PeerPingHealthCheckStatus

  case class UnknownPeer(id: Id) extends PeerPingHealthCheckStatus

  sealed trait MissingProposalHealthCheckStatus extends HealthCheckStatus

  object MissingProposalHealthCheckStatus {
    implicit val missingProposalHealthCheckStatusEncoder: Encoder[MissingProposalHealthCheckStatus] = deriveEncoder
    implicit val missingProposalHealthCheckStatusDecoder: Decoder[MissingProposalHealthCheckStatus] = deriveDecoder
  }

  case class GotPeerProposalAtHeight(id: Id, height: Long) extends MissingProposalHealthCheckStatus
  case class MissingPeerProposalAtHeight(id: Id, height: Long) extends MissingProposalHealthCheckStatus
  case class MissingOwnProposalAtHeight(id: Id, height: Long) extends MissingProposalHealthCheckStatus
}
