package org.constellation.migrations

import org.constellation.schema.snapshot.{SnapshotInfo, SnapshotInfoV1}

object SnapshotInfoV1Migration {
  def convert(old: SnapshotInfoV1): SnapshotInfo =
    SnapshotInfo(
      snapshot = old.snapshot,
      acceptedCBSinceSnapshot = old.acceptedCBSinceSnapshot,
      acceptedCBSinceSnapshotCache = old.acceptedCBSinceSnapshotCache,
      awaitingCbs = old.awaitingCbs,
      lastSnapshotHeight = old.lastSnapshotHeight,
      snapshotHashes = old.snapshotHashes,
      addressCacheData = old.addressCacheData,
      tips = old.tips,
      snapshotCache = old.snapshotCache,
      lastAcceptedTransactionRef = old.lastAcceptedTransactionRef,
      tipUsages = Map()
    )
}
