package org.constellation.datastore

import org.constellation.consensus
import org.constellation.primitives.Schema._

/** Databse trait. */
trait Datastore {

  // doc
  def restart(): Unit

  // doc
  def delete(key: String): Boolean

  // doc
  def getSnapshot(key: String): Option[consensus.Snapshot]

  // doc
  def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit

  // doc
  def putCheckpointCacheData(key: String, c: CheckpointCacheData): Unit

  // doc
  def updateCheckpointCacheData(key: String,
                                f: CheckpointCacheData => CheckpointCacheData,
                                empty: CheckpointCacheData): CheckpointCacheData

  // doc
  def getCheckpointCacheData(key: String): Option[CheckpointCacheData]

  // doc
  def putTransactionCacheData(key: String, t: TransactionCacheData): Unit

  // doc
  def updateTransactionCacheData(
                                  key: String,
                                  f: TransactionCacheData => TransactionCacheData,
                                  empty: TransactionCacheData
                                ): TransactionCacheData

  // doc
  def getTransactionCacheData(key: String): Option[TransactionCacheData]

  // doc
  def putAddressCacheData(key: String, t: AddressCacheData): Unit

  // doc
  def updateAddressCacheData(key: String,
                             f: AddressCacheData => AddressCacheData,
                             empty: AddressCacheData): AddressCacheData

  // doc
  def getAddressCacheData(key: String): Option[AddressCacheData]

  // doc
  def putSignedObservationEdgeCache(key: String,
                                    t: SignedObservationEdgeCache): Unit

  // doc
  def updateSignedObservationEdgeCache(
                                        key: String,
                                        f: SignedObservationEdgeCache => SignedObservationEdgeCache,
                                        empty: SignedObservationEdgeCache
                                      ): SignedObservationEdgeCache

  // doc
  def getSignedObservationEdgeCache(
                                     key: String
                                   ): Option[SignedObservationEdgeCache]

  // doc
  def putTransactionEdgeData(key: String, t: TransactionEdgeData): Unit

  // doc
  def updateTransactionEdgeData(key: String,
                                f: TransactionEdgeData => TransactionEdgeData,
                                empty: TransactionEdgeData): TransactionEdgeData

  // doc
  def getTransactionEdgeData(key: String): Option[TransactionEdgeData]

  // doc
  def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit

  // doc
  def updateCheckpointEdgeData(key: String,
                               f: CheckpointEdgeData => CheckpointEdgeData,
                               empty: CheckpointEdgeData): CheckpointEdgeData

  // doc
  def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData]

} // end Datastore trait
