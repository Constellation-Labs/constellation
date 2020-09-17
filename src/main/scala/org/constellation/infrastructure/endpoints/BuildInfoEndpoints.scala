package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import org.constellation.BuildInfo
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._

class BuildInfoEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {
  import BuildInfoEndpoints.BuildInfoJson
  import BuildInfoEndpoints.BuildInfoJson._

  private def buildInfoEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "buildInfo" => Ok(BuildInfoJson().asJson)
    }

  private def gitCommitEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "buildInfo" / "gitCommit" => Ok(BuildInfo.gitCommit.asJson)
    }

  def peerEndpoints() = buildInfoEndpoint() <+> gitCommitEndpoint()

  def ownerEndpoints() = buildInfoEndpoint() <+> gitCommitEndpoint()

}

object BuildInfoEndpoints {

  def peerEndpoints[F[_]: Concurrent](): HttpRoutes[F] =
    new BuildInfoEndpoints[F].peerEndpoints()

  def ownerEndpoints[F[_]: Concurrent](): HttpRoutes[F] =
    new BuildInfoEndpoints[F].ownerEndpoints()

  case class BuildInfoJson(
    name: String,
    version: String,
    scalaVersion: String,
    sbtVersion: String,
    gitBranch: String,
    gitCommit: String,
    builtAtString: String,
    builtAtMillis: Long
  )

  object BuildInfoJson {

    implicit val buildInfoJsonEncoder: Encoder[BuildInfoJson] = deriveEncoder
    implicit val buildInfoJsonDecoder: Decoder[BuildInfoJson] = deriveDecoder

    def apply(): BuildInfoJson = new BuildInfoJson(
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.scalaVersion,
      BuildInfo.sbtVersion,
      BuildInfo.gitBranch,
      BuildInfo.gitCommit,
      BuildInfo.builtAtString,
      BuildInfo.builtAtMillis
    )
  }
}
