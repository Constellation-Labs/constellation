package org.constellation.domain.snapshotInfo

object SnapshotInfoChunk extends Enumeration {

  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  type SnapshotInfoChunk = Value

  protected case class Val(value: String) extends super.Val {
    def name: String = value
  }

  val SNAPSHOT: SnapshotInfoChunk = Val("snapshot")
  val STORED_SNAPSHOT_CHECKPOINT_BLOCKS: SnapshotInfoChunk = Val("storedSnapshotCheckpointBlocks")
  val LAST_SNAPSHOT_HEIGHT: SnapshotInfoChunk = Val("lastSnapshotHeight")
  val CHECKPOINT_BLOCKS: SnapshotInfoChunk = Val("checkpointBlocks")
  val ACCEPTED_CBS_SINCE_SNAPSHOT: SnapshotInfoChunk = Val("acceptedCBSinceSnapshot")
  val ACCEPTED_CBS_SINCE_SNAPSHOT_CACHE: SnapshotInfoChunk = Val("acceptedCBSinceSnapshotCache")
  val AWAITING_CBS: SnapshotInfoChunk = Val("awaitingCbs")
  val SNAPSHOT_HASHES: SnapshotInfoChunk = Val("snapshotHashes")
  val ADDRESS_CACHE_DATA: SnapshotInfoChunk = Val("addressCacheData")
  val TIPS: SnapshotInfoChunk = Val("tips")
  val SNAPSHOT_CACHE: SnapshotInfoChunk = Val("snapshotCache")
  val LAST_ACCEPTED_TX_REF: SnapshotInfoChunk = Val("lastAcceptedTransactionRef")
  val PUBLIC_REPUTATION: SnapshotInfoChunk = Val("publicReputation")
  val SNAPSHOT_OWN: SnapshotInfoChunk = Val("snapshotOwn")
  val SNAPSHOT_FETCH_PROPOSALS: SnapshotInfoChunk = Val("snapshotFetchProposals")
}
