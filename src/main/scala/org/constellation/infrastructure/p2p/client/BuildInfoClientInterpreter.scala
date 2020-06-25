package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.BuildInfoClientAlgebra
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.http4s.client.Client
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityDecoder._
import scala.language.reflectiveCalls

class BuildInfoClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F], sessionTokenService: SessionTokenService[F]) extends BuildInfoClientAlgebra[F] {

  import BuildInfoJson._

  def getBuildInfo() = PeerResponse[F, BuildInfoJson]("buildInfo")(client)

  def getGitCommit() = PeerResponse[F, String]("buildInfo/gitCommit")(client, sessionTokenService)
}

object BuildInfoClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F], sessionTokenService: SessionTokenService[F]): BuildInfoClientInterpreter[F] =
    new BuildInfoClientInterpreter[F](client, sessionTokenService: SessionTokenService[F])
}
