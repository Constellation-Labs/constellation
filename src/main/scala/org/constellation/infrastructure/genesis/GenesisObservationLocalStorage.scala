package org.constellation.infrastructure.genesis

import cats.effect.{Concurrent, Sync}
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.primitives.Schema.GenesisObservation

class GenesisObservationLocalStorage[F[_]: Concurrent](baseDir: String)(implicit F: Sync[F])
    extends LocalFileStorage[F, GenesisObservation](baseDir) {}

object GenesisObservationLocalStorage {

  def apply[F[_]: Concurrent](baseDir: String): GenesisObservationLocalStorage[F] =
    new GenesisObservationLocalStorage[F](baseDir)
}
