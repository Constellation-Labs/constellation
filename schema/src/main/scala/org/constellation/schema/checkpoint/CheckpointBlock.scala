package org.constellation.schema.checkpoint

import java.security.KeyPair

import io.circe.Decoder
import org.constellation.schema._
import org.constellation.schema.edge.{Edge, EdgeHashType, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.schema.observation.Observation
import org.constellation.schema.signature.HashSignature
import org.constellation.schema.transaction.Transaction

import org.constellation.schema.signature.SignHelp.signedObservationEdge

abstract class CheckpointEdgeLike(val checkpoint: CheckpointEdge) {
  def baseHash: String = checkpoint.edge.baseHash

  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

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
  def hash: String = checkpoint.edge.observationEdge.hash
  // TODO: Optimize call, should store this value instead of recalculating every time.
  def soeHash: String = checkpoint.edge.signedObservationEdge.hash
  def signaturesHash: String = checkpoint.edge.signedObservationEdge.signatureBatch.hash

  def validHash = hash == signaturesHash

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

  import io.circe.Encoder
  import io.circe.generic.semiauto._
  import io.circe.syntax._

  implicit val checkpointBlockEncoder: Encoder[CheckpointBlock] = deriveEncoder
  implicit val checkpointBlockDecoder: Decoder[CheckpointBlock] = deriveDecoder

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

  def checkpointToJsonString(checkpointBlock: CheckpointBlock): String =
    checkpointBlock.asJson.noSpaces

}
