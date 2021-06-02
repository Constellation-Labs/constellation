package org.constellation.migrations

import org.constellation.schema.snapshot.{SnapshotInfo, SnapshotInfoV1}

object SnapshotInfoV1Migration {

  def convert(old: SnapshotInfoV1): SnapshotInfo =
    SnapshotInfo(
      snapshot = old.snapshot,
      lastSnapshotHeight = old.lastSnapshotHeight,
      nextSnapshotHash = "",
      checkpoints = Map.empty,
      waitingForAcceptance = Set.empty,
      accepted = Set.empty,
      awaiting = Set.empty,
      inSnapshot = Set.empty,
      addressCacheData = old.addressCacheData,
      lastAcceptedTransactionRef = old.lastAcceptedTransactionRef,
      tips = old.tips.keySet,
      usages = Map()

//      acceptedCBSinceSnapshot = old.acceptedCBSinceSnapshot.toSet,
//      acceptedCBSinceSnapshotCache = old.acceptedCBSinceSnapshotCache,
//      awaitingCheckpoints = old.awaitingCbs.map(_.checkpointBlock.soeHash),
//      snapshotHashes = old.snapshotHashes,
//      snapshotCache = old.snapshotCache,
    )
}
