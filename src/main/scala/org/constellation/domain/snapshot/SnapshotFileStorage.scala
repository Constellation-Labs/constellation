package org.constellation.domain.snapshot

import cats.effect.Concurrent
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.storage.FileStorage

abstract class SnapshotFileStorage[F[_]: Concurrent](baseDir: String) extends FileStorage[F, StoredSnapshot](baseDir) {}
