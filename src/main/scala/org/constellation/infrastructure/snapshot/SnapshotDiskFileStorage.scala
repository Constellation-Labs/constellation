package org.constellation.infrastructure.snapshot

import cats.effect.Concurrent
import org.constellation.domain.snapshot.SnapshotFileStorage

class SnapshotDiskFileStorage[F[_]](baseDir: String)(implicit F: Concurrent[F])
    extends SnapshotFileStorage[F](baseDir) {}

object SnapshotDiskFileStorage {
  def apply[F[_]: Concurrent](baseDir: String): SnapshotDiskFileStorage[F] = new SnapshotDiskFileStorage[F](baseDir)
}
