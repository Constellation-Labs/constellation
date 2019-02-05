package org.constellation.datastore

import org.constellation.consensus
import org.constellation.primitives.Schema._

trait KVDBDatastoreImpl extends Datastore {

  val kvdb: KVDB

  import kvdb._

  override def restart(): Unit = restart()

  override def delete(key: String): Boolean = delete(key)

  override def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit = put(key, snapshot)

  override def getSnapshot(key: String): Option[consensus.Snapshot] = get[consensus.Snapshot](key)

  override def putCheckpointCacheData(s: String, c: CheckpointCacheData): Unit =
    put(s, c)

  override def getCheckpointCacheData(s: String): Option[CheckpointCacheData] =
    get[CheckpointCacheData](s)

  override def putTransactionCacheData(s: String,
                                       t: TransactionCacheData): Unit =
    put(s, t)

  override def getTransactionCacheData(
    s: String
  ): Option[TransactionCacheData] = get[TransactionCacheData](s)

  override def updateCheckpointCacheData(
    key: String,
    f: CheckpointCacheData => CheckpointCacheData,
    empty: CheckpointCacheData
  ): CheckpointCacheData = update(key, f, empty)

  override def updateTransactionCacheData(
    key: String,
    f: TransactionCacheData => TransactionCacheData,
    empty: TransactionCacheData
  ): TransactionCacheData = update(key, f, empty)

  override def putAddressCacheData(key: String, t: AddressCacheData): Unit =
    put(key, t)

  override def updateAddressCacheData(
    key: String,
    f: AddressCacheData => AddressCacheData,
    empty: AddressCacheData
  ): AddressCacheData = update(key, f, empty)

  override def getAddressCacheData(key: String): Option[AddressCacheData] = {
    get[AddressCacheData](key)
  }

  override def putSignedObservationEdgeCache(
    key: String,
    t: SignedObservationEdgeCache
  ): Unit =
    put(key, t)

  override def updateSignedObservationEdgeCache(
    key: String,
    f: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: SignedObservationEdgeCache
  ): SignedObservationEdgeCache =
    update(key, f, empty)

  override def getSignedObservationEdgeCache(
    key: String
  ): Option[SignedObservationEdgeCache] =
    get[SignedObservationEdgeCache](key)

  override def putTransactionEdgeData(key: String,
                                      t: TransactionEdgeData): Unit =
    put(key, t)

  override def updateTransactionEdgeData(
    key: String,
    f: TransactionEdgeData => TransactionEdgeData,
    empty: TransactionEdgeData
  ): TransactionEdgeData = update(key, f, empty)

  override def getTransactionEdgeData(
    key: String
  ): Option[TransactionEdgeData] = get[TransactionEdgeData](key)

  override def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit =
    put(key, t)

  override def updateCheckpointEdgeData(
    key: String,
    f: CheckpointEdgeData => CheckpointEdgeData,
    empty: CheckpointEdgeData
  ): CheckpointEdgeData = update(key, f, empty)

  override def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData] =
    get[CheckpointEdgeData](key)
}
