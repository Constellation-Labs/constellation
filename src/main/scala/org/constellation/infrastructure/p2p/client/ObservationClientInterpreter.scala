package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.ObservationClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.v2.observation.Observation
import org.constellation.session.SessionTokenService
import org.http4s.Method._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class ObservationClientInterpreter[F[_]: Concurrent: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
) extends ObservationClientAlgebra[F] {

  def getObservation(hash: String): PeerResponse[F, Option[Observation]] =
    PeerResponse[F, Option[Observation]](s"observation/$hash")(client, sessionTokenService)

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[Observation])]] =
    PeerResponse(s"batch/observations", POST)(client, sessionTokenService) { (req, c) =>
      c.expect[List[(String, Option[Observation])]](req.withEntity(hashes))
    }

}

object ObservationClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): ObservationClientInterpreter[F] =
    new ObservationClientInterpreter[F](client, sessionTokenService)
}
