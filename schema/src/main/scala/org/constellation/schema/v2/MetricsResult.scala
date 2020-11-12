package org.constellation.schema.v2

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class MetricsResult(metrics: Map[String, String])

object MetricsResult {
  implicit val metricsResultEncoder: Encoder[MetricsResult] = deriveEncoder
  implicit val metricsResultDecoder: Decoder[MetricsResult] = deriveDecoder
}
