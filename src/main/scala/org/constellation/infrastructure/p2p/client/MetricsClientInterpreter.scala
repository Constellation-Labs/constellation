package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.MetricsClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.MetricsResult
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class MetricsClientInterpreter[F[_]: ContextShift](client: Client[F], sessionTokenService: SessionTokenService[F])(
  implicit F: Concurrent[F]
) extends MetricsClientAlgebra[F] {

  def checkHealth(): PeerResponse[F, Unit] =
    PeerResponse.successful[F]("health", "Cannot check health")(client, sessionTokenService)//should we be checking token for health? I guess we should

  def getMetrics(): PeerResponse[F, MetricsResult] =
    PeerResponse[F, MetricsResult]("metrics")(client, sessionTokenService)
}

object MetricsClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): MetricsClientInterpreter[F] =
    new MetricsClientInterpreter[F](client, sessionTokenService)
}
