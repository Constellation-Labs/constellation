package org.constellation.primitives

import java.security.KeyPair

import cats.effect.IO
import cats.implicits._
import constellation.signedObservationEdge
import org.constellation.DAO
import org.constellation.domain.observation.Observation
import org.constellation.p2p.PeerNotification
import org.constellation.primitives.Schema._
import org.constellation.schema.Id
import org.constellation.util.HashSignature

abstract class CheckpointEdgeLike(val checkpoint: CheckpointEdge) {
  def baseHash: String = checkpoint.edge.baseHash

  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
    checkpoint.edge.parentHashes.flatMap { key =>
      dao.soeService.lookup(key).unsafeRunSync
    }.map(_.baseHash)

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def signatures: Seq[HashSignature] =
    checkpoint.edge.signedObservationEdge.signatureBatch.signatures
}

case class CheckpointBlockMetadata(
  transactionsMerkleRoot: Option[String],
  checkpointEdge: CheckpointEdge,
  messagesMerkleRoot: Option[String],
  notificationsMerkleRoot: Option[String],
  observationsMerkleRoot: Option[String]
) extends CheckpointEdgeLike(checkpointEdge)

case class CheckpointBlock(
  transactions: Seq[Transaction],
  checkpoint: CheckpointEdge,
  messages: Seq[ChannelMessage] = Seq(),
  notifications: Seq[PeerNotification] = Seq(),
  observations: Seq[Observation] = Seq()
) {

  def uniqueSignatures: Boolean = signatures.groupBy(_.id).forall(_._2.size == 1)

  def signedBy(id: Id): Boolean = witnessIds.contains(id)

  def hashSignaturesOf(id: Id): Seq[HashSignature] = signatures.filter(_.id == id)

  def signatureConflict(other: CheckpointBlock): Boolean =
    signatures.exists { s =>
      other.signatures.exists { s2 =>
        s.signature != s2.signature && s.id == s2.id
      }
    }

  def witnessIds: Seq[Id] = signatures.map { _.id }

  def signatures: Seq[HashSignature] =
    checkpoint.edge.signedObservationEdge.signatureBatch.signatures

  def baseHash: String = checkpoint.edge.baseHash

  def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

  // TODO: Optimize call, should store this value instead of recalculating every time.

  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def plus(keyPair: KeyPair): CheckpointBlock =
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignatureFrom(keyPair)))

  def plus(hs: HashSignature): CheckpointBlock =
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignature(hs)))

  def plus(other: CheckpointBlock): CheckpointBlock =
    this.copy(
      checkpoint = checkpoint.plus(other.checkpoint),
      transactions = transactions ++ other.transactions,
      messages = messages ++ other.messages,
      notifications = notifications ++ other.notifications,
      observations = observations ++ other.observations
    )

  def +(other: CheckpointBlock): CheckpointBlock = plus(other)

  def plusEdge(other: CheckpointBlock): CheckpointBlock =
    this.copy(
      checkpoint = checkpoint.plus(other.checkpoint)
    )

  def parentSOE: Seq[TypedEdgeHash] = checkpoint.edge.parents

  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

}

object CheckpointBlock {

  def createCheckpointBlockSOE(
    transactions: Seq[Transaction],
    tips: Seq[SignedObservationEdge],
    messages: Seq[ChannelMessage] = Seq.empty,
    peers: Seq[PeerNotification] = Seq.empty,
    observations: Seq[Observation] = Seq.empty
  )(implicit keyPair: KeyPair): CheckpointBlock =
    createCheckpointBlock(transactions, tips.map { t =>
      TypedEdgeHash(t.hash, EdgeHashType.CheckpointHash)
    }, messages, peers, observations)

  def createCheckpointBlock(
    transactions: Seq[Transaction],
    tips: Seq[TypedEdgeHash],
    messages: Seq[ChannelMessage] = Seq.empty,
    peers: Seq[PeerNotification] = Seq.empty,
    observations: Seq[Observation] = Seq.empty
  )(implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData =
      CheckpointEdgeData(
        transactions.map(_.hash).sorted,
        messages.map(_.signedMessageData.hash).sorted,
        observations.map(_.hash).sorted
      )

    val observationEdge = ObservationEdge(
      tips.toList,
      TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash)
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, checkpointEdgeData)
    )

    CheckpointBlock(transactions, checkpointEdge, messages, peers, observations)
  }

}
