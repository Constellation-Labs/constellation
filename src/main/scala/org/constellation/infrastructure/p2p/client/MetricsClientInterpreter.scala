package org.constellation.infrastructure.p2p.client

import cats.effect.Concurrent
import io.circe.generic.auto._
import org.constellation.domain.p2p.client.MetricsClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.MetricsResult
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.Method._

class MetricsClientInterpreter[F[_]](client: Client[F])(implicit F: Concurrent[F]) extends MetricsClientAlgebra[F] {

  def checkHealth(): PeerResponse[F, Unit] =
    PeerResponse.successful[F]("health", "Cannot check health")(client)

  def getMetrics(): PeerResponse[F, MetricsResult] =
    PeerResponse[F, MetricsResult]("metrics")(client)
}

object MetricsClientInterpreter {

  def apply[F[_]: Concurrent](client: Client[F]): MetricsClientInterpreter[F] =
    new MetricsClientInterpreter[F](client)
}
