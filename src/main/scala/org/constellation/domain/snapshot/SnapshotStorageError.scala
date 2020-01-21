package org.constellation.domain.snapshot

trait SnapshotStorageError {
  def cause: String
}
