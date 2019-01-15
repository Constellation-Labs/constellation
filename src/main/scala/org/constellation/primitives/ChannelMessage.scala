package org.constellation.primitives

import java.security.KeyPair

import org.constellation.DAO
import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.util.{ProductHash, SignatureBatch}
import constellation._

case class ChannelMessageData(
                               message: String,
                               previousMessageDataHash: String,
                               channelId: String
                             ) extends ProductHash

case class SignedData[+D <: ProductHash](
                                          data: D,
                                          signatures: SignatureBatch
                                        ) extends ProductHash

case class ChannelMessage(signedMessageData: SignedData[ChannelMessageData])

object ChannelMessage {
  def create(message: String, previous: String, channelId: String)(implicit dao: DAO): ChannelMessage = {
    val data = ChannelMessageData(message, previous, channelId)
    ChannelMessage(
      SignedData(data, hashSignBatchZeroTyped(data, dao.keyPair))
    )
  }
}


// TODO: Parent references?
/*

case class ChannelMessage(
                         oeWithValues: SignedData[ChannelMessageData]
                         )
object ChannelMessage {

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
