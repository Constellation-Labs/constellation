package org.constellation.datastore

import org.constellation.consensus
import org.constellation.primitives.Schema._

/** Key-value database implementation trait. */
trait KVDBDatastoreImpl extends Datastore {

  val kvdb: KVDB

  import kvdb._

  /** Reset. */
  override def restart(): Unit = restart()

  /** Delete entry corresponding to input key. */
  override def delete(key: String): Boolean = delete(key)

  // doc
  override def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit = put(key, snapshot)

  // doc
  override def getSnapshot(key: String): Option[consensus.Snapshot] = get[consensus.Snapshot](key)

  // doc
  override def putCheckpointCacheData(s: String, c: CheckpointCacheData): Unit =
    put(s, c)

  // doc
  override def getCheckpointCacheData(s: String): Option[CheckpointCacheData] =
    get[CheckpointCacheData](s)

  // doc
  override def putTransactionCacheData(s: String,
                                       t: TransactionCacheData): Unit =
    put(s, t)

  // doc
  override def getTransactionCacheData(
                                        s: String
                                      ): Option[TransactionCacheData] = get[TransactionCacheData](s)

  // doc
  override def updateCheckpointCacheData(
                                          key: String,
                                          f: CheckpointCacheData => CheckpointCacheData,
                                          empty: CheckpointCacheData
                                        ): CheckpointCacheData = update(key, f, empty)

  // doc
  override def updateTransactionCacheData(
                                           key: String,
                                           f: TransactionCacheData => TransactionCacheData,
                                           empty: TransactionCacheData
                                         ): TransactionCacheData = update(key, f, empty)

  // doc
  override def putAddressCacheData(key: String, t: AddressCacheData): Unit =
    put(key, t)

  // doc
  override def updateAddressCacheData(
                                       key: String,
                                       f: AddressCacheData => AddressCacheData,
                                       empty: AddressCacheData
                                     ): AddressCacheData = update(key, f, empty)

  // doc
  override def getAddressCacheData(key: String): Option[AddressCacheData] = {
    get[AddressCacheData](key)
  }

  // doc
  override def putSignedObservationEdgeCache(
                                              key: String,
                                              t: SignedObservationEdgeCache
                                            ): Unit =
    put(key, t)

  // doc
  override def updateSignedObservationEdgeCache(
                                                 key: String,
                                                 f: SignedObservationEdgeCache => SignedObservationEdgeCache,
                                                 empty: SignedObservationEdgeCache
                                               ): SignedObservationEdgeCache =
    update(key, f, empty)

  // doc
  override def getSignedObservationEdgeCache(
                                              key: String
                                            ): Option[SignedObservationEdgeCache] =
    get[SignedObservationEdgeCache](key)

  // doc
  override def putTransactionEdgeData(key: String,
                                      t: TransactionEdgeData): Unit =
    put(key, t)

  // doc
  override def updateTransactionEdgeData(
                                          key: String,
                                          f: TransactionEdgeData => TransactionEdgeData,
                                          empty: TransactionEdgeData
                                        ): TransactionEdgeData = update(key, f, empty)

  // doc
  override def getTransactionEdgeData(
                                       key: String
                                     ): Option[TransactionEdgeData] = get[TransactionEdgeData](key)

  // doc
  override def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit =
    put(key, t)

  // doc
  override def updateCheckpointEdgeData(
                                         key: String,
                                         f: CheckpointEdgeData => CheckpointEdgeData,
                                         empty: CheckpointEdgeData
                                       ): CheckpointEdgeData = update(key, f, empty)

  // doc
  override def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData] =
    get[CheckpointEdgeData](key)

} // end KVDBDatastoreImpl trait

