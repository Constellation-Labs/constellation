package org.constellation.primitives

import java.security.KeyPair

import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema._
import org.constellation.util.{HashSignature, Signable}

/** Documentation. */
case class Edge[+D <: Signable]
(
  observationEdge: ObservationEdge,
  signedObservationEdge: SignedObservationEdge,
  data: D
) {

  /** Documentation. */
  def baseHash: String = signedObservationEdge.signatureBatch.hash

  /** Documentation. */
  def parentHashes: Seq[String] = observationEdge.parents.map(_.hash)

  /** Documentation. */
  def parents: Seq[TypedEdgeHash] = observationEdge.parents

  /** Documentation. */
  def storeTransactionCacheData(db: Datastore, update: TransactionCacheData => TransactionCacheData, empty: TransactionCacheData, resolved: Boolean = false): Unit = {
    db.updateTransactionCacheData(signedObservationEdge.baseHash, update, empty)
    db.putSignedObservationEdgeCache(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
    db.putTransactionEdgeData(data.hash, data.asInstanceOf[TransactionEdgeData])
  }

  /** Documentation. */
  def storeCheckpointData(db: Datastore, update: CheckpointCacheData => CheckpointCacheData, empty: CheckpointCacheData, resolved: Boolean = false): Unit = {
    db.updateCheckpointCacheData(signedObservationEdge.baseHash, update, empty)
    db.putSignedObservationEdgeCache(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
    db.putCheckpointEdgeData(data.hash, data.asInstanceOf[CheckpointEdgeData])
  }

  /** Documentation. */
  def withSignatureFrom(keyPair: KeyPair): Edge[D] = {
    this.copy(signedObservationEdge = signedObservationEdge.withSignatureFrom(keyPair))
  }

  /** Documentation. */
  def withSignature(hs: HashSignature): Edge[D] = {
    this.copy(signedObservationEdge = signedObservationEdge.withSignature(hs))
  }

  /** Documentation. */
  def plus(other: Edge[_]): Edge[D] = {
    this.copy(signedObservationEdge = signedObservationEdge.plus(other.signedObservationEdge))
  }

}
