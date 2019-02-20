package org.constellation.primitives

import java.util.concurrent.Semaphore

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.core.report.ProcessingReport
import com.github.fge.jsonschema.main.{JsonSchemaFactory, JsonValidator}
import com.typesafe.scalalogging.Logger
import org.json4s.jackson.JsonMethods.{asJsonNode, parse}
import constellation._
import org.constellation.DAO
import org.constellation.util.{MerkleProof, Signable, SignatureBatch}

import scala.concurrent.Future

// Should channelId be associated with a unique keyPair or not?

case class ChannelMessageData(
  message: String,
  previousMessageHash: String,
  channelId: String
) extends Signable

case class SignedData[+D <: Signable](
  data: D,
  signatures: SignatureBatch
) extends Signable

case class ChannelMessageMetadata(
  channelMessage: ChannelMessage,
  blockHash: Option[String] = None,
  snapshotHash: Option[String] = None
)

case class ChannelMetadata(
  channelOpen: ChannelOpen,
  genesisMessageMetadata: ChannelMessageMetadata,
  totalNumMessages: Long = 0L
)

case class ChannelMessage(signedMessageData: SignedData[ChannelMessageData])

object ChannelMessage {

  val logger = Logger("ChannelMessage")

  def create(message: String, previous: String, channelId: String)(
    implicit dao: DAO
  ): ChannelMessage = {
    val data = ChannelMessageData(message, previous, channelId)
    ChannelMessage(
      SignedData(data, hashSignBatchZeroTyped(data, dao.keyPair))
    )
  }

  def createGenesis(
    channelOpenRequest: ChannelOpen
  )(implicit dao: DAO): Future[ChannelOpenResponse] = {

    logger.info(s"Channel open $channelOpenRequest")

    dao.threadSafeMessageMemPool.selfChannelNameToGenesisMessage
      .get(channelOpenRequest.name)
      .map { msg =>
        Future.successful(
          ChannelOpenResponse("Error: channel name already in use", msg.signedMessageData.hash)
        )
      }
      .getOrElse {
        val genesisMessageStr = channelOpenRequest.json
        val msg = create(genesisMessageStr, Genesis.CoinBaseHash, channelOpenRequest.name)
        dao.threadSafeMessageMemPool.selfChannelNameToGenesisMessage(channelOpenRequest.name) = msg
        val genesisHashChannelId = msg.signedMessageData.hash
        dao.threadSafeMessageMemPool.selfChannelIdToName(genesisHashChannelId) =
          channelOpenRequest.name
        dao.threadSafeMessageMemPool.put(Seq(msg), overrideLimit = true)
        val semaphore = new Semaphore(1)
        dao.threadSafeMessageMemPool.activeChannels(genesisHashChannelId) = semaphore
        semaphore.acquire()
        Future {
          var retries = 0
          var metadata: Option[ChannelMetadata] = None
          while (retries < 10 && metadata.isEmpty) {
            retries += 1
            Thread.sleep(1000)
            metadata = dao.channelService.get(genesisHashChannelId)
          }
          val response =
            if (metadata.isEmpty) "Timeout awaiting block acceptance"
            else {
              "Success"
            }
          ChannelOpenResponse(response, genesisHashChannelId)
        }(dao.edgeExecutionContext)
      }
  }

  def createMessages(
    channelSendRequest: ChannelSendRequest
  )(implicit dao: DAO): Future[ChannelSendResponse] = {

    dao.messageService
      .get(channelSendRequest.channelId)
      .map { previousMessage =>
        val previous = previousMessage.channelMessage.signedMessageData.hash

        val messages = channelSendRequest.messages
          .foldLeft(previous -> Seq[ChannelMessage]()) {
            case ((prvHash, signedMessages), nextMessage) =>
              val nextSigned = create(nextMessage, previous, channelSendRequest.channelId)
              nextSigned.signedMessageData.hash -> (signedMessages :+ nextSigned)
          }
          ._2
        dao.threadSafeMessageMemPool.put(messages, overrideLimit = true)
        val semaphore = new Semaphore(1)
        dao.threadSafeMessageMemPool.activeChannels(channelSendRequest.channelId) = semaphore
        semaphore.acquire()
        Future.successful(
          ChannelSendResponse(
            "Success",
            messages.map { _.signedMessageData.hash }
          )
        )
      }
      .getOrElse(
        Future.successful(
          ChannelSendResponse("Channel not found", Seq())
        )
      )
  }
}

case class ChannelProof(
  channelMessageMetadata: ChannelMessageMetadata,
  // snapshotProof: MerkleProof,
  checkpointProof: MerkleProof,
  checkpointMessageProof: MerkleProof
)

case class ChannelOpen(
  name: String,
  jsonSchema: Option[String] = None,
  acceptInvalid: Boolean = true
)

case class ChannelOpenResponse(errorMessage: String = "Success", genesisHash: String = "")

case class ChannelSendRequest(
  channelId: String,
  messages: Seq[String]
)

case class ChannelSendRequestRawJson(channelId: String, messages: String)

case class ChannelSendResponse(
  errorMessage: String = "Success",
  messageHashes: Seq[String]
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
