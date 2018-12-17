package org.constellation.primitives

import java.security.KeyPair

import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.util.ProductHash


case class ChannelMessageData(
                         message: String,
                         previousMessageTXHash: String,
                         channelId: String
                         ) extends ProductHash

// Intended replacement for 'Edge' class since we're not really using parent data right now anyways.
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
    val oe = ObservationEdge(parents.head, parents(1), Some(TypedEdgeHash(data.hash, EdgeHashType.ChannelMessageDataHash)))
    val soe = constellation.signedObservationEdge(oe)
    ChannelMessage(ObservationEdgeWithValues(
      oe, soe, data
    ))

  }

}
