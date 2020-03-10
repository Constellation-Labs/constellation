package org.constellation.domain.storage

object StorageItemKind extends Enumeration {
  type StorageItemKind = Value

  val GenesisObservation, Snapshot, SnapshotInfo, EigenTrust = Value

  def toSuffix(kind: StorageItemKind): String = kind match {
    case GenesisObservation => "genesis_observation"
    case Snapshot           => "snapshot"
    case SnapshotInfo       => "snapshot_info"
    case EigenTrust         => "eigen_trust"
  }
}
