package org.constellation.infrastructure.snapshot

import cats.effect.Concurrent
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.schema.v2.snapshot.StoredSnapshot

class SnapshotLocalStorage[F[_]: Concurrent](baseDir: String) extends LocalFileStorage[F, StoredSnapshot](baseDir) {}

object SnapshotLocalStorage {
  def apply[F[_]: Concurrent](baseDir: String): SnapshotLocalStorage[F] = new SnapshotLocalStorage[F](baseDir)
}
