package org.constellation.datastore

import org.constellation.consensus
import org.constellation.primitives.Schema._
import org.constellation.primitives.TransactionCacheData

trait Datastore {

  def restart(): Unit

  def delete(key: String): Boolean

  def getSnapshot(key: String): Option[consensus.Snapshot]

  def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit

  def putCheckpointCacheData(key: String, c: CheckpointCacheData): Unit

  def updateCheckpointCacheData(key: String,
                                f: CheckpointCacheData => CheckpointCacheData,
                                empty: CheckpointCacheData): CheckpointCacheData

  def getCheckpointCacheData(key: String): Option[CheckpointCacheData]

  def putTransactionCacheData(key: String, t: TransactionCacheData): Unit

  def updateTransactionCacheData(
    key: String,
    f: TransactionCacheData => TransactionCacheData,
    empty: TransactionCacheData
  ): TransactionCacheData

  def getTransactionCacheData(key: String): Option[TransactionCacheData]

  def putAddressCacheData(key: String, t: AddressCacheData): Unit

  def updateAddressCacheData(key: String,
                             f: AddressCacheData => AddressCacheData,
                             empty: AddressCacheData): AddressCacheData

  def getAddressCacheData(key: String): Option[AddressCacheData]

  def putSignedObservationEdgeCache(key: String, t: SignedObservationEdgeCache): Unit

  def updateSignedObservationEdgeCache(
    key: String,
    f: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: SignedObservationEdgeCache
  ): SignedObservationEdgeCache

  def getSignedObservationEdgeCache(
    key: String
  ): Option[SignedObservationEdgeCache]

  def putTransactionEdgeData(key: String, t: TransactionEdgeData): Unit

  def updateTransactionEdgeData(key: String,
                                f: TransactionEdgeData => TransactionEdgeData,
                                empty: TransactionEdgeData): TransactionEdgeData

  def getTransactionEdgeData(key: String): Option[TransactionEdgeData]

  def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit

  def updateCheckpointEdgeData(key: String,
                               f: CheckpointEdgeData => CheckpointEdgeData,
                               empty: CheckpointEdgeData): CheckpointEdgeData

  def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData]
}
