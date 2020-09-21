package org.constellation.schema.snapshot

import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.{CheckpointCache, TipData}
import org.constellation.schema.transaction.LastTransactionRef

case class SnapshotInfo(
  snapshot: StoredSnapshot,
  acceptedCBSinceSnapshot: Seq[String] = Seq(),
  acceptedCBSinceSnapshotCache: Seq[CheckpointCache] = Seq(),
  awaitingCbs: Set[CheckpointCache] = Set(),
  lastSnapshotHeight: Int = 0,
  snapshotHashes: Seq[String] = Seq(),
  addressCacheData: Map[String, AddressCacheData] = Map(),
  tips: Map[String, TipData] = Map(),
  snapshotCache: Seq[CheckpointCache] = Seq(),
  lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map()
)
