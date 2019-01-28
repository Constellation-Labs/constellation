package org.constellation.primitives

import constellation._
import org.constellation.DAO
import org.constellation.util.{MerkleProof, ProductHash, SignatureBatch}

/** Channel message data wrapper. */
case class ChannelMessageData(
                               message: String,
                               previousMessageDataHash: String,
                               channelId: String
                             ) extends ProductHash

// doc
case class SignedData[+D <: ProductHash](
                                          data: D,
                                          signatures: SignatureBatch
                                        ) extends ProductHash

/** Channel message meta-data wrapper. */
case class ChannelMessageMetadata(
                                   channelMessage: ChannelMessage,
                                   blockHash: Option[String] = None,
                                   snapshotHash: Option[String] = None
                                 )

// doc
case class ChannelMessage(signedMessageData: SignedData[ChannelMessageData])

/** Channel message. */
object ChannelMessage {

  /** Use SignedData class to wrap input data. */
  def create(message: String, previous: String, channelId: String)(implicit dao: DAO): ChannelMessage = {
    val data = ChannelMessageData(message, previous, channelId)
    ChannelMessage(
      SignedData(data, hashSignBatchZeroTyped(data, dao.keyPair))
    )
  }
}

// doc
case class ChannelProof(
                         channelMessageMetadata: ChannelMessageMetadata,
                         // snapshotProof: MerkleProof, // tmp comment
                         checkpointProof: MerkleProof,
                         checkpointMessageProof: MerkleProof
                       )

// TODO: Parent references?
/*
  // doc
  case class ChannelMessage(
                            oeWithValues: SignedData[ChannelMessageData]
                           )
  // doc
  object ChannelMessage {

  // doc
  def apply(
             message: String, previous: String, channelId: String, parents: Seq[TypedEdgeHash]
           )(implicit kp: KeyPair): ChannelMessage = {

    val data = ChannelMessageData(message, previous, channelId)
    val oe = ObservationEdge(parents.head, parents(1), Some(TypedEdgeHash(data.hash, EdgeHashType.ChannelMessageDataHash)))
    val soe = constellation.signedObservationEdge(oe)
    ChannelMessage(ObservationEdgeWithValues(
      oe, soe, data
    ))
  }
}
*/
