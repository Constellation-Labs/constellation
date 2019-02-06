package org.constellation.primitives

import java.util.concurrent.Semaphore
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.core.report.ProcessingReport
import com.github.fge.jsonschema.main.{JsonSchemaFactory, JsonValidator}
import org.json4s.jackson.JsonMethods.{asJsonNode, parse}

import constellation._
import org.constellation.DAO
import org.constellation.util.{MerkleProof, Signable, SignatureBatch}

// Should channelId be associated with a unique keyPair or not?

case class ChannelMessageData(
  message: String,
  previousMessageDataHash: String,
  channelId: String
) extends Signable

case class ChannelOpen(
  jsonSchema: Option[String] = None,
  allowInvalid: Boolean = true
)

case class SignedData[+D <: Signable](
  data: D,
  signatures: SignatureBatch
) extends Signable

case class ChannelMessageMetadata(
  channelMessage: ChannelMessage,
  blockHash: Option[String] = None,
  snapshotHash: Option[String] = None
)

case class ChannelMessage(signedMessageData: SignedData[ChannelMessageData])

object ChannelMessage {

  def create(message: String, previous: String, channelId: String)(
    implicit dao: DAO): ChannelMessage = {
    val data = ChannelMessageData(message, previous, channelId)
    ChannelMessage(
      SignedData(data, hashSignBatchZeroTyped(data, dao.keyPair))
    )
  }

  def createGenesis(channelOpenRequest: ChannelOpenRequest)(
    implicit dao: DAO): Option[ChannelMessage] = {
    if (dao.messageService.get(channelOpenRequest.channelId).isEmpty &&
        !dao.threadSafeMessageMemPool.activeChannels.contains(channelOpenRequest.channelId)) {

      val genesisMessageStr =
        ChannelOpen(channelOpenRequest.jsonSchema, channelOpenRequest.acceptInvalid).json
      val msg = create(genesisMessageStr, Genesis.CoinBaseHash, channelOpenRequest.channelId)
      val semaphore = new Semaphore(1)
      dao.threadSafeMessageMemPool.activeChannels(channelOpenRequest.channelId) = semaphore
      semaphore.acquire()
      dao.threadSafeMessageMemPool.put(Seq(msg), overrideLimit = true)
      Some(msg)
    } else None
  }

  def createMessages(channelSendRequest: ChannelSendRequest)(
    implicit dao: DAO): Seq[ChannelMessage] = {
    // Ignores locking right now
    val previous =
      dao.messageService.get(channelSendRequest.channelId).get.channelMessage.signedMessageData.hash
    val messages = channelSendRequest.messages
      .foldLeft(previous -> Seq[ChannelMessage]()) {
        case ((prvHash, signedMessages), nextMessage) =>
          val nextSigned = create(nextMessage, previous, channelSendRequest.channelId)
          nextSigned.signedMessageData.hash -> (signedMessages :+ nextSigned)
      }
      ._2
    dao.threadSafeMessageMemPool.put(messages, overrideLimit = true)
    messages
  }
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
)

case class ChannelSendRequest(
  channelId: String,
  messages: Seq[String]
)

case class SensorData(
  temperature: Int,
  name: String
)

object SensorData {

  val jsonSchema: String = """{
                     |  "title":"Sensors data",
                     |  "type":"object",
                     |  "properties":{
                     |    "temperature": {
                     |      "type": "integer",
                     |      "minimum": -100,
                     |      "maximum": 100
                     |    },
                     |    "name": {
                     |      "type": "string",
                     |      "pattern": "^[A-Z]{4,10}$"
                     |    }
                     |  },
                     |  "required":["temperature", "name"]
                     |}""".stripMargin

  val schema: JsonNode = asJsonNode(parse(jsonSchema))
  val validator: JsonValidator = JsonSchemaFactory.byDefault().getValidator

  def validate(input: String): ProcessingReport =
    validator.validate(schema, asJsonNode(parse(input)))

  //def validate()

}

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
