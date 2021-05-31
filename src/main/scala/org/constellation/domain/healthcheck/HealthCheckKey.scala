package org.constellation.domain.healthcheck

import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import org.constellation.schema.Id

sealed trait HealthCheckKey {
  val id: Id
}

object HealthCheckKey {
  // TODO: For the classes with the same fields this decoder would not be deterministic and would always decode into first matching class
  //       type annotation is required
  implicit val healthCheckKeyEncoder: Encoder[HealthCheckKey] = Encoder.instance {
    case a: MissingProposalHealthCheckKey => a.asJson
    case a: PingHealthCheckKey            => a.asJson
  }

  implicit val healthCheckKeyDecoder: Decoder[HealthCheckKey] = List[Decoder[HealthCheckKey]](
    Decoder[MissingProposalHealthCheckKey].widen,
    Decoder[PingHealthCheckKey].widen
  ).reduceLeft(_.or(_))

  case class PingHealthCheckKey(id: Id) extends HealthCheckKey

  object PingHealthCheckKey {
    implicit val pingHealthCheckKeyEncoder: Encoder[PingHealthCheckKey] = deriveEncoder
    implicit val pingHealthCheckKeyDecoder: Decoder[PingHealthCheckKey] = deriveDecoder
  }
  case class MissingProposalHealthCheckKey(id: Id, height: Long) extends HealthCheckKey

  object MissingProposalHealthCheckKey {
    implicit val missingProposalHealthCheckKeyEncoder: Encoder[MissingProposalHealthCheckKey] = deriveEncoder
    implicit val missingProposalHealthCheckKeyDecoder: Decoder[MissingProposalHealthCheckKey] = deriveDecoder
  }
}
