package org.constellation.schema.v2.snapshot

import org.constellation.schema.v2.address.AddressCacheData
import org.constellation.schema.v2.checkpoint.{CheckpointCache, TipData}
import org.constellation.schema.v2.transaction.LastTransactionRef

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
