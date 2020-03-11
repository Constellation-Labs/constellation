package org.constellation.infrastructure.rewards

import cats.effect.{Concurrent, Sync}
import org.constellation.domain.rewards.StoredEigenTrust
import org.constellation.domain.storage.FileStorage

class EigenTrustLocalStorage[F[_]: Concurrent](baseDir: String)(implicit F: Sync[F])
    extends FileStorage[F, StoredEigenTrust](baseDir) {}

object EigenTrustLocalStorage {
  def apply[F[_]: Concurrent](dirPath: String): EigenTrustLocalStorage[F] = new EigenTrustLocalStorage[F](dirPath)
}
