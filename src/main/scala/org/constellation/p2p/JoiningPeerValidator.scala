package org.constellation.p2p

import cats.data.ValidatedNel
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.serializer.KryoSerializer
import org.constellation.{BuildInfo, ConfigUtil, PeerMetadata}
import constellation._

class JoiningPeerValidator[F[_]: Concurrent](apiClient: ClientInterpreter[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private type ValidationResult[A] = ValidatedNel[JoiningPeerValidationMessage, A]

  def isValid(peerClientMetadata: PeerClientMetadata): F[Boolean] =
    validation(peerClientMetadata).map(_.isValid)

  def validation(peerClientMetadata: PeerClientMetadata): F[ValidationResult[String]] =
    validateBuildInfo(peerClientMetadata)

  private def validateBuildInfo(peerClientMetadata: PeerClientMetadata): F[ValidationResult[String]] = {
    val validate: F[ValidationResult[String]] = for {
      peerBuildInfo <- apiClient.buildInfo.getBuildInfo().run(peerClientMetadata)
      buildInfo = BuildInfoJson()

      peerBuildInfoSerialized = KryoSerializer.serializeAnyRef(peerBuildInfo).sha256
      _ <- logger.debug(s"Joining peer build info hash=$peerBuildInfoSerialized")

      buildInfoSerialized = KryoSerializer.serializeAnyRef(buildInfo).sha256
      _ <- logger.debug(s"Node build info hash=$buildInfoSerialized")

      isValid = peerBuildInfoSerialized == buildInfoSerialized
    } yield
      if (!isValid) JoiningPeerHasDifferentVersion(peerClientMetadata.host).invalidNel
      else peerClientMetadata.host.validNel

    validate.handleErrorWith(
      error =>
        logger.info(s"Cannot get build info of joining peer : ${peerClientMetadata.host} : $error") >>
          Sync[F].delay(JoiningPeerUnavailable(peerClientMetadata.host).invalidNel)
    )
  }
}

object JoiningPeerValidator {

  def apply[F[_]: Concurrent](apiClient: ClientInterpreter[F]): JoiningPeerValidator[F] =
    new JoiningPeerValidator(apiClient)
}
