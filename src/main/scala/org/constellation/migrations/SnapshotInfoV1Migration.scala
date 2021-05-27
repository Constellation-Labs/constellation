package org.constellation.migrations

import org.constellation.schema.snapshot.{SnapshotInfo, SnapshotInfoV1}

object SnapshotInfoV1Migration {
  def convert(old: SnapshotInfoV1): SnapshotInfo =
    SnapshotInfo(
      snapshot = old.snapshot,
      acceptedCBSinceSnapshot = old.acceptedCBSinceSnapshot.toSet,
      acceptedCBSinceSnapshotCache = old.acceptedCBSinceSnapshotCache,
      awaitingCheckpoints = old.awaitingCbs.map(_.checkpointBlock.soeHash),
      lastSnapshotHeight = old.lastSnapshotHeight,
      snapshotHashes = old.snapshotHashes,
      addressCacheData = old.addressCacheData,
      tips = old.tips.keySet,
      snapshotCache = old.snapshotCache,
      lastAcceptedTransactionRef = old.lastAcceptedTransactionRef,
      tipUsages = Map()
    )
}
