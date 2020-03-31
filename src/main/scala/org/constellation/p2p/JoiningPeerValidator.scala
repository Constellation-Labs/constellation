package org.constellation.p2p

import cats.data.ValidatedNel
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.{BuildInfo, ConfigUtil, PeerMetadata}

class JoiningPeerValidator[F[_]: Concurrent](apiClient: ClientInterpreter[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private type ValidationResult[A] = ValidatedNel[JoiningPeerValidationMessage, A]

  def isValid(peerClientMetadata: PeerClientMetadata): F[Boolean] =
    validation(peerClientMetadata).map(_.isValid)

  def validation(peerClientMetadata: PeerClientMetadata): F[ValidationResult[String]] =
    validateGitCommitHash(peerClientMetadata)

  private def validateGitCommitHash(peerClientMetadata: PeerClientMetadata): F[ValidationResult[String]] = {
    val validate: F[ValidationResult[String]] = for {
      peerGitCommitBuild <- apiClient.buildInfo.getGitCommit().run(peerClientMetadata)
      _ <- logger.debug(s"Joining peer git commit hash=$peerGitCommitBuild")

      nodeGitCommitBuild = BuildInfo.gitCommit
      _ <- logger.debug(s"Node peer git commit hash=$nodeGitCommitBuild")

      isValid = peerGitCommitBuild == nodeGitCommitBuild
      _ <- logger.info(s"Checking commit hash for joining peer=${peerClientMetadata.host} : is valid=$isValid")
    } yield
      if (!isValid) JoiningPeerHasDifferentVersion(peerClientMetadata.host).invalidNel
      else peerClientMetadata.host.validNel

    validate.handleErrorWith(
      error =>
        logger.info(s"Cannot get git commit hash from joining peer : ${peerClientMetadata.host} : $error") >>
          Sync[F].delay(JoiningPeerUnavailable(peerClientMetadata.host).invalidNel)
    )
  }
}

object JoiningPeerValidator {

  def apply[F[_]: Concurrent](apiClient: ClientInterpreter[F]): JoiningPeerValidator[F] =
    new JoiningPeerValidator(apiClient)
}
