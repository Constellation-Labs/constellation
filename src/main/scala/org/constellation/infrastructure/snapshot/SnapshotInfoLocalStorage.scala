package org.constellation.infrastructure.snapshot

import cats.effect.{Concurrent, Sync}
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.schema.v2.snapshot.SnapshotInfo

class SnapshotInfoLocalStorage[F[_]: Concurrent](baseDir: String)(implicit F: Sync[F])
    extends LocalFileStorage[F, SnapshotInfo](baseDir) {}

object SnapshotInfoLocalStorage {

  def apply[F[_]: Concurrent](baseDir: String): SnapshotInfoLocalStorage[F] =
    new SnapshotInfoLocalStorage[F](baseDir)
}
