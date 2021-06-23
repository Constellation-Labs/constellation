package org.constellation.migrations

import org.constellation.schema.checkpoint.{CheckpointCache, CheckpointCacheV1}
import org.constellation.schema.snapshot.{SnapshotInfo, SnapshotInfoV1, StoredSnapshot, StoredSnapshotV1}

object SnapshotInfoV1Migration {

  def convertBlock(old: CheckpointCacheV1): CheckpointCache =
    CheckpointCache(
      old.checkpointBlock,
      old.children,
      old.height.get
    )

  def convertStoredSnapshot(old: StoredSnapshotV1): StoredSnapshot =
    StoredSnapshot(
      old.snapshot,
      old.checkpointCache.map(convertBlock)
    )

  def convert(old: SnapshotInfoV1): SnapshotInfo =
    SnapshotInfo(
      snapshot = convertStoredSnapshot(old.snapshot),
      lastSnapshotHeight = old.lastSnapshotHeight,
      nextSnapshotHash = old.snapshot.snapshot.hash,
      checkpoints = (old.acceptedCBSinceSnapshotCache ++ old.awaitingCbs ++ old.tips.values.map(
        tip => CheckpointCacheV1(tip.checkpointBlock, height = Some(tip.height))
      )).map(convertBlock)
        .map(cb => (cb.checkpointBlock.soeHash, cb))
        .toMap,
      waitingForAcceptance = old.awaitingCbs.map(_.checkpointBlock.soeHash),
      accepted = old.acceptedCBSinceSnapshotCache.map(_.checkpointBlock.soeHash).toSet,
      awaiting = Set.empty,
      inSnapshot = old.snapshot.checkpointCache.map(cb => (cb.checkpointBlock.soeHash, old.snapshot.height)).toSet,
      addressCacheData = old.addressCacheData,
      lastAcceptedTransactionRef = old.lastAcceptedTransactionRef,
      tips = old.tips.map { case (_, v) => v.checkpointBlock.soeHash }.toSet,
      usages = Map.empty
    )
}
