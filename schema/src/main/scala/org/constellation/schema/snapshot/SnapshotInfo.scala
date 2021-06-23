package org.constellation.schema.snapshot

import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.{CheckpointCache, CheckpointCacheV1, TipData}
import org.constellation.schema.transaction.LastTransactionRef

case class SnapshotInfo(
  snapshot: StoredSnapshot,
  lastSnapshotHeight: Int = 0,
  nextSnapshotHash: String = "",
  checkpoints: Map[String, CheckpointCache] = Map.empty,
  waitingForAcceptance: Set[String] = Set.empty,
  accepted: Set[String] = Set.empty,
  awaiting: Set[String] = Set.empty,
  inSnapshot: Set[(String, Long)] = Set.empty,
  addressCacheData: Map[String, AddressCacheData] = Map.empty,
  lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map.empty,
  tips: Set[String] = Set.empty,
  usages: Map[String, Set[String]] = Map.empty
)

case class SnapshotInfoV1(
  snapshot: StoredSnapshotV1,
  acceptedCBSinceSnapshot: Seq[String] = Seq.empty,
  acceptedCBSinceSnapshotCache: Seq[CheckpointCacheV1] = Seq.empty,
  awaitingCbs: Set[CheckpointCacheV1] = Set.empty,
  lastSnapshotHeight: Int = 0,
  snapshotHashes: Seq[String] = Seq.empty,
  addressCacheData: Map[String, AddressCacheData] = Map.empty,
  tips: Map[String, TipData] = Map.empty,
  snapshotCache: Seq[CheckpointCacheV1] = Seq.empty,
  lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map.empty
)
