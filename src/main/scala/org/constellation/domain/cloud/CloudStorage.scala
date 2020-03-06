package org.constellation.domain.cloud

import better.files.File
import org.constellation.domain.cloud.CloudStorage.StorageName.StorageName

trait CloudStorage[F[_]] {
  def upload(files: Seq[File], storageName: StorageName): F[List[String]]
  def get(filename: String, storageName: StorageName): F[Array[Byte]]
}

object CloudStorage {

  object StorageName extends Enumeration {
    type StorageName = Value

    val Genesis, Snapshot, SnapshotInfo = Value

    def toDirectory(sn: StorageName): String = sn match {
      case Genesis      => "genesis"
      case Snapshot     => "snapshots"
      case SnapshotInfo => "snapshot-infos"
    }
  }
}
