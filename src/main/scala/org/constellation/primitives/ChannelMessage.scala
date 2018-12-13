package org.constellation.primitives

import java.security.KeyPair

import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.util.ProductHash


case class ChannelMessageData(
                         message: String,
                         previousMessageTXHash: String,
                         channelMerkleRoot: String
                         ) extends ProductHash

case class ObservationEdgeWithValues[+D <: ProductHash](
                                       oe: ObservationEdge,
                                       soe: SignedObservationEdge,
                                       data: D
                                       )

case class ChannelMessage(
                         oeWithValues: ObservationEdgeWithValues[ChannelMessageData]
                         )

object ChannelMessage {

  def apply(
             message: String, previous: String, root: String, parents: Seq[TypedEdgeHash]
           )(implicit kp: KeyPair): ChannelMessage = {

    val data = ChannelMessageData(message, previous, root)
    val oe = ObservationEdge(parents.head, parents(1), Some(TypedEdgeHash(data.hash, EdgeHashType.ChannelMessageHash)))
    val soe = constellation.signedObservationEdge(oe)
    ChannelMessage(ObservationEdgeWithValues(
      oe, soe, data
    ))

  }

}
