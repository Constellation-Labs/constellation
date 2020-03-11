package org.constellation.infrastructure.snapshot

import cats.effect.Concurrent
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.storage.FileStorage

class SnapshotLocalStorage[F[_]: Concurrent](baseDir: String) extends FileStorage[F, StoredSnapshot](baseDir) {}

object SnapshotLocalStorage {
  def apply[F[_]: Concurrent](baseDir: String): SnapshotLocalStorage[F] = new SnapshotLocalStorage[F](baseDir)
}
