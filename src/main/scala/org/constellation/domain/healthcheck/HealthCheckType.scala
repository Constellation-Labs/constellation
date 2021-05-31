package org.constellation.domain.healthcheck

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

sealed trait HealthCheckType

object HealthCheckType {
  implicit val healthCheckTypeCodec: Codec[HealthCheckType] = deriveCodec

  case object PingHealthCheck extends HealthCheckType
  case object MissingProposalHealthCheck extends HealthCheckType
}
