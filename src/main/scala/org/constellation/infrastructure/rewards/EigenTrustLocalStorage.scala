package org.constellation.infrastructure.rewards

import cats.effect.{Concurrent, Sync}
import org.constellation.domain.rewards.StoredEigenTrust
import org.constellation.domain.storage.LocalFileStorage

class EigenTrustLocalStorage[F[_]: Concurrent](baseDir: String)(implicit F: Sync[F])
    extends LocalFileStorage[F, StoredEigenTrust](baseDir) {}

object EigenTrustLocalStorage {

  def apply[F[_]: Concurrent](baseDir: String): EigenTrustLocalStorage[F] =
    new EigenTrustLocalStorage[F](baseDir)
}
