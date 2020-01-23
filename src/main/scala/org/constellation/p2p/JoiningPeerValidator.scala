package org.constellation.p2p

import cats.data.ValidatedNel
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.util.APIClient
import org.constellation.{BuildInfo, ConfigUtil}

class JoiningPeerValidator[F[_]: Concurrent](implicit cs: ContextShift[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private type ValidationResult[A] = ValidatedNel[JoiningPeerValidationMessage, A]

  def isValid(client: APIClient): F[Boolean] =
    validation(client).map(_.isValid)

  def validation(client: APIClient): F[ValidationResult[String]] =
    validateGitCommitHash(client)

  private def validateGitCommitHash(client: APIClient): F[ValidationResult[String]] = {
    val validate: F[ValidationResult[String]] = for {
      peerGitCommitBuildResponse <- client.getStringF("buildInfo/gitCommit")(cs)
      peerGitCommitBuild = peerGitCommitBuildResponse.body.getOrElse("")
      _ <- logger.debug(s"Joining peer git commit hash=$peerGitCommitBuild")

      nodeGitCommitBuild = BuildInfo.gitCommit
      _ <- logger.debug(s"Node peer git commit hash=$nodeGitCommitBuild")

      isValid = peerGitCommitBuild == nodeGitCommitBuild
      _ <- logger.info(s"Checking commit hash for joining peer=${client.hostName} : is valid=$isValid")
    } yield if (!isValid) JoiningPeerHasDifferentVersion(client.hostName).invalidNel else client.hostName.validNel

    validate.handleErrorWith(
      error =>
        logger.info(s"Cannot get git commit hash from joining peer : ${client.hostName} : $error") >>
          Sync[F].delay(JoiningPeerUnavailable(client.hostName).invalidNel)
    )
  }
}

object JoiningPeerValidator {

  def apply[F[_]: Concurrent: ContextShift]: JoiningPeerValidator[F] =
    new JoiningPeerValidator
}
