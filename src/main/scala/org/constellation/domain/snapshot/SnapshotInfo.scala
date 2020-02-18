package org.constellation.domain.snapshot

import org.constellation.consensus.{StoredSnapshot, TipData}
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.primitives.Schema.{AddressCacheData, CheckpointCache}

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

object SnapshotInfo {}
