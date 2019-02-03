package org.constellation.datastore

import org.constellation.consensus
import org.constellation.primitives.Schema._

/** Documentation. */
trait KVDBDatastoreImpl extends Datastore {

  val kvdb: KVDB

  import kvdb._

  /** Documentation. */
  override def restart(): Unit = restart()

  /** Documentation. */
  override def delete(key: String): Boolean = delete(key)

  /** Documentation. */
  override def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit = put(key, snapshot)

  /** Documentation. */
  override def getSnapshot(key: String): Option[consensus.Snapshot] = get[consensus.Snapshot](key)

  /** Documentation. */
  override def putCheckpointCacheData(s: String, c: CheckpointCacheData): Unit =
    put(s, c)

  /** Documentation. */
  override def getCheckpointCacheData(s: String): Option[CheckpointCacheData] =
    get[CheckpointCacheData](s)

  /** Documentation. */
  override def putTransactionCacheData(s: String,
                                       t: TransactionCacheData): Unit =
    put(s, t)

  /** Documentation. */
  override def getTransactionCacheData(
    s: String
  ): Option[TransactionCacheData] = get[TransactionCacheData](s)

  /** Documentation. */
  override def updateCheckpointCacheData(
    key: String,
    f: CheckpointCacheData => CheckpointCacheData,
    empty: CheckpointCacheData
  ): CheckpointCacheData = update(key, f, empty)

  /** Documentation. */
  override def updateTransactionCacheData(
    key: String,
    f: TransactionCacheData => TransactionCacheData,
    empty: TransactionCacheData
  ): TransactionCacheData = update(key, f, empty)

  /** Documentation. */
  override def putAddressCacheData(key: String, t: AddressCacheData): Unit =
    put(key, t)

  /** Documentation. */
  override def updateAddressCacheData(
    key: String,
    f: AddressCacheData => AddressCacheData,
    empty: AddressCacheData
  ): AddressCacheData = update(key, f, empty)

  /** Documentation. */
  override def getAddressCacheData(key: String): Option[AddressCacheData] = {
    get[AddressCacheData](key)
  }

  /** Documentation. */
  override def putSignedObservationEdgeCache(
    key: String,
    t: SignedObservationEdgeCache
  ): Unit =
    put(key, t)

  /** Documentation. */
  override def updateSignedObservationEdgeCache(
    key: String,
    f: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: SignedObservationEdgeCache
  ): SignedObservationEdgeCache =
    update(key, f, empty)

  /** Documentation. */
  override def getSignedObservationEdgeCache(
    key: String
  ): Option[SignedObservationEdgeCache] =
    get[SignedObservationEdgeCache](key)

  /** Documentation. */
  override def putTransactionEdgeData(key: String,
                                      t: TransactionEdgeData): Unit =
    put(key, t)

  /** Documentation. */
  override def updateTransactionEdgeData(
    key: String,
    f: TransactionEdgeData => TransactionEdgeData,
    empty: TransactionEdgeData
  ): TransactionEdgeData = update(key, f, empty)

  /** Documentation. */
  override def getTransactionEdgeData(
    key: String
  ): Option[TransactionEdgeData] = get[TransactionEdgeData](key)

  /** Documentation. */
  override def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit =
    put(key, t)

  /** Documentation. */
  override def updateCheckpointEdgeData(
    key: String,
    f: CheckpointEdgeData => CheckpointEdgeData,
    empty: CheckpointEdgeData
  ): CheckpointEdgeData = update(key, f, empty)

  /** Documentation. */
  override def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData] =
    get[CheckpointEdgeData](key)
}

