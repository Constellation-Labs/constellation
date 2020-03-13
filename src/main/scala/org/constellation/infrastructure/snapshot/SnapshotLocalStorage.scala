package org.constellation.infrastructure.snapshot

import cats.effect.Concurrent
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.storage.LocalFileStorage

class SnapshotLocalStorage[F[_]: Concurrent](baseDir: String) extends LocalFileStorage[F, StoredSnapshot](baseDir) {}

object SnapshotLocalStorage {
  def apply[F[_]: Concurrent](baseDir: String): SnapshotLocalStorage[F] = new SnapshotLocalStorage[F](baseDir)
}
