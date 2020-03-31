package org.constellation.infrastructure.p2p.client

import cats.effect.Concurrent
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.domain.observation.Observation
import org.constellation.domain.p2p.client.ObservationClientAlgebra
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

class ObservationClientInterpreter[F[_]: Concurrent](client: Client[F]) extends ObservationClientAlgebra[F] {

  def getObservation(hash: String): PeerResponse[F, Option[Observation]] =
    PeerResponse[F, Option[Observation]](s"observation/$hash")(client)

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[Observation])]] =
    PeerResponse(s"batch/observation", POST) { req =>
      client.expect[List[(String, Option[Observation])]](req.withEntity(hashes))
    }

}

object ObservationClientInterpreter {

  def apply[F[_]: Concurrent](client: Client[F]): ObservationClientInterpreter[F] =
    new ObservationClientInterpreter[F](client)
}
