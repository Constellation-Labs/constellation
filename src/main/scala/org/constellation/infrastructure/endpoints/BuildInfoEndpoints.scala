package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.BuildInfo
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import io.circe.generic.auto._
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._

class BuildInfoEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

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

  private def buildInfoEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "buildInfo" => Ok(BuildInfoJson().asJson)
    }

  private def gitCommitEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "gitCommit" => Ok(BuildInfo.gitCommit)
    }

  def peerEndpoints() = buildInfoEndpoint() <+> gitCommitEndpoint()

  def ownerEndpoints() = buildInfoEndpoint() <+> gitCommitEndpoint()

}

object BuildInfoEndpoints {

  def peerEndpoints[F[_]: Concurrent](): HttpRoutes[F] =
    new BuildInfoEndpoints[F].peerEndpoints()

  def ownerEndpoints[F[_]: Concurrent](): HttpRoutes[F] =
    new BuildInfoEndpoints[F].ownerEndpoints()
}
