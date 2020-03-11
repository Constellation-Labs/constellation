package org.constellation.infrastructure.snapshot

import cats.effect.{Concurrent, Sync}
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.storage.FileStorage

class SnapshotInfoLocalStorage[F[_]: Concurrent](baseDir: String)(implicit F: Sync[F])
    extends FileStorage[F, SnapshotInfo](baseDir) {}

object SnapshotInfoLocalStorage {
  def apply[F[_]: Concurrent](dirPath: String): SnapshotInfoLocalStorage[F] = new SnapshotInfoLocalStorage[F](dirPath)
}
