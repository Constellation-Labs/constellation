package org.constellation.schema.snapshot

import com.esotericsoftware.kryo.serializers.TaggedFieldSerializer.Tag
import org.constellation.schema.Height
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.{CheckpointCache, TipData}
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
  addressCacheData: Map[String, AddressCacheData] = Map(),
  lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map(),
  tips: Set[String] = Set.empty,
  usages: Map[String, Set[String]] = Map()
)

case class SnapshotInfoV1(
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
