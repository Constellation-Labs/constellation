package org.constellation.domain.healthcheck

import cats.effect.Sync
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class PrefixedHealthCheckLogger[F[_]: Sync](healthCheckType: HealthCheckType) {
  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  val prefix = s"[$healthCheckType] "

  def error(message: String): F[Unit] =
    logger.error(prefix + message)

  def error(t: Throwable)(message: String): F[Unit] =
    logger.error(t)(prefix + message)

  def warn(message: String): F[Unit] =
    logger.warn(prefix + message)

  def warn(t: Throwable)(message: String): F[Unit] =
    logger.warn(t)(prefix + message)

  def info(message: String): F[Unit] =
    logger.info(prefix + message)

  def info(t: Throwable)(message: String): F[Unit] =
    logger.info(t)(prefix + message)

  def debug(message: String): F[Unit] =
    logger.debug(prefix + message)

  def debug(t: Throwable)(message: String): F[Unit] =
    logger.debug(t)(prefix + message)
}
