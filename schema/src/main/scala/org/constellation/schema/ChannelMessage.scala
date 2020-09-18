package org.constellation.schema

import java.security.KeyPair
import java.util.concurrent.Semaphore

import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema
import org.constellation.schema.merkle.MerkleProof
import org.constellation.schema.signature.{Signable, SignatureBatch}

import scala.concurrent.Future

// Should channelId be associated with a unique keyPair or not?

case class ChannelMessageData(
  message: String,
  previousMessageHash: String,
  channelId: String
) extends Signable

object ChannelMessageData {
  implicit val channelMessageDataEncoder: Encoder[ChannelMessageData] = deriveEncoder
  implicit val channelMessageDataDecoder: Decoder[ChannelMessageData] = deriveDecoder
}

case class SignedData[+D <: Signable](
  data: D,
  signatures: SignatureBatch
) extends Signable

case class ChannelMessageMetadata(
  channelMessage: ChannelMessage,
  blockHash: Option[String] = None,
  snapshotHash: Option[String] = None
)

object ChannelMessageMetadata {
  implicit val channelMessageMetadata: Encoder[ChannelMessageMetadata] = deriveEncoder
}

case class ChannelMetadata(
  channelOpen: ChannelOpen,
  genesisMessageMetadata: ChannelMessageMetadata,
  totalNumMessages: Long = 0L,
  last25MessageHashes: Seq[String] = Seq()
) {
  def channelId = genesisMessageMetadata.channelMessage.signedMessageData.hash
}

case class SingleChannelUIOutput(
  channelOpen: ChannelOpen,
  totalNumMessages: Long = 0L,
  last25MessageHashes: Seq[String] = Seq(),
  genesisAddress: String
)

case class ChannelMessage(signedMessageData: SignedData[ChannelMessageData])

object ChannelMessage extends StrictLogging {
  implicit val signedChannelMessageDataEncoder: Encoder[SignedData[ChannelMessageData]] = deriveEncoder
  implicit val signedChannelMessageDataDecoder: Decoder[SignedData[ChannelMessageData]] = deriveDecoder

  implicit val channelMessageEncoder: Encoder[ChannelMessage] = deriveEncoder
  implicit val channelMessageDecoder: Decoder[ChannelMessage] = deriveDecoder
}

case class ChannelProof(
  channelMessageMetadata: ChannelMessageMetadata,
  // snapshotProof: MerkleProof,
  checkpointProof: MerkleProof,
  checkpointMessageProof: MerkleProof
)

case class ChannelOpenRequest(
  channelId: String,
  jsonSchema: Option[String] = None,
  acceptInvalid: Boolean = true
) extends ChannelRequest
case class ChannelOpen(
  name: String,
  jsonSchema: Option[String] = None,
  acceptInvalid: Boolean = true
)

case class ChannelOpenResponse(
  errorMessage: String = "Success",
  genesisHash: String = ""
)

case class ChannelSendRequest(
  channelId: String,
  messages: Seq[String]
) extends ChannelRequest

trait ChannelRequest {
  val channelId: String
}

case class ChannelSendRequestRawJson(channelId: String, messages: String)

case class ChannelSendResponse(
  errorMessage: String = "Success",
  messageHashes: Seq[String]
)

// TODO: Switch to Parent references?
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
