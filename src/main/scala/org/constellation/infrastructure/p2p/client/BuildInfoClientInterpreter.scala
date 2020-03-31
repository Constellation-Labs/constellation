package org.constellation.infrastructure.p2p.client

import cats.effect.Concurrent
import org.constellation.domain.p2p.client.BuildInfoClientAlgebra
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.infrastructure.p2p.PeerResponse
import org.http4s.circe.CirceEntityDecoder._

class BuildInfoClientInterpreter[F[_]: Concurrent](client: Client[F]) extends BuildInfoClientAlgebra[F] {

  def getBuildInfo() = PeerResponse[F, BuildInfoJson]("buildInfo")(client)

  def getGitCommit() = PeerResponse[F, String]("buildInfo/gitCommit")(client)
}

object BuildInfoClientInterpreter {

  def apply[F[_]: Concurrent](client: Client[F]): BuildInfoClientInterpreter[F] =
    new BuildInfoClientInterpreter[F](client)
}
