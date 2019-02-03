package org.constellation.datastore

import org.constellation.consensus
import org.constellation.primitives.Schema._

/** Documentation. */
trait Datastore {

  /** Documentation. */
  def restart(): Unit

  /** Documentation. */
  def delete(key: String): Boolean

  /** Documentation. */
  def getSnapshot(key: String) : Option[consensus.Snapshot]

  /** Documentation. */
  def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit

  /** Documentation. */
  def putCheckpointCacheData(key: String, c: CheckpointCacheData): Unit

  /** Documentation. */
  def updateCheckpointCacheData(key: String,
                                f: CheckpointCacheData => CheckpointCacheData,
                                empty: CheckpointCacheData): CheckpointCacheData

  /** Documentation. */
  def getCheckpointCacheData(key: String): Option[CheckpointCacheData]

  /** Documentation. */
  def putTransactionCacheData(key: String, t: TransactionCacheData): Unit

  /** Documentation. */
  def updateTransactionCacheData(
    key: String,
    f: TransactionCacheData => TransactionCacheData,
    empty: TransactionCacheData
  ): TransactionCacheData

  /** Documentation. */
  def getTransactionCacheData(key: String): Option[TransactionCacheData]

  /** Documentation. */
  def putAddressCacheData(key: String, t: AddressCacheData): Unit

  /** Documentation. */
  def updateAddressCacheData(key: String,
                             f: AddressCacheData => AddressCacheData,
                             empty: AddressCacheData): AddressCacheData

  /** Documentation. */
  def getAddressCacheData(key: String): Option[AddressCacheData]

  /** Documentation. */
  def putSignedObservationEdgeCache(key: String,
                                    t: SignedObservationEdgeCache): Unit

  /** Documentation. */
  def updateSignedObservationEdgeCache(
    key: String,
    f: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: SignedObservationEdgeCache
  ): SignedObservationEdgeCache

  /** Documentation. */
  def getSignedObservationEdgeCache(
    key: String
  ): Option[SignedObservationEdgeCache]

  /** Documentation. */
  def putTransactionEdgeData(key: String, t: TransactionEdgeData): Unit

  /** Documentation. */
  def updateTransactionEdgeData(key: String,
                                f: TransactionEdgeData => TransactionEdgeData,
                                empty: TransactionEdgeData): TransactionEdgeData

  /** Documentation. */
  def getTransactionEdgeData(key: String): Option[TransactionEdgeData]

  /** Documentation. */
  def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit

  /** Documentation. */
  def updateCheckpointEdgeData(key: String,
                               f: CheckpointEdgeData => CheckpointEdgeData,
                               empty: CheckpointEdgeData): CheckpointEdgeData

  /** Documentation. */
  def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData]
}

