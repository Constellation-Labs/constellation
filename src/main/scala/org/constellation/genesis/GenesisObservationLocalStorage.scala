package org.constellation.genesis

import cats.data.EitherT
import cats.implicits._
import cats.effect.Concurrent
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.primitives.Schema.GenesisObservation

class GenesisObservationLocalStorage[F[_]: Concurrent](baseDir: String)
    extends LocalFileStorage[F, GenesisObservation](baseDir) {

  private val logger = Slf4jLogger.getLogger[F]

  def write(genesisObservation: GenesisObservation): EitherT[F, Throwable, Unit] =
    write("genesisObservation", genesisObservation)

  def read(): EitherT[F, Throwable, GenesisObservation] =
    read("genesisObservation")
}

object GenesisObservationLocalStorage {

  def apply[F[_]: Concurrent](path: String): GenesisObservationLocalStorage[F] =
    new GenesisObservationLocalStorage[F](path)
}
