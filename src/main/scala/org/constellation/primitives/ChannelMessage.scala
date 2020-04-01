package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.Semaphore

import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.util.{MerkleProof, Signable, SignatureBatch}

import scala.concurrent.Future
import scala.util.Random

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

  def create(message: String, previous: String, channelId: String)(
    implicit keyPair: KeyPair
  ): ChannelMessage = {
    val data = ChannelMessageData(message, previous, channelId)
    ChannelMessage(
      SignedData(data, hashSignBatchZeroTyped(data, keyPair))
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
        logger.info(s"Channel not in use")

        val genesisMessageStr = "" // TODO: channelOpenRequest.json
        val msg = create(genesisMessageStr, Genesis.Coinbase, channelOpenRequest.name)(dao.keyPair)
        dao.threadSafeMessageMemPool.selfChannelNameToGenesisMessage(channelOpenRequest.name) = msg
        val genesisHashChannelId = msg.signedMessageData.hash
        dao.threadSafeMessageMemPool.selfChannelIdToName(genesisHashChannelId) = channelOpenRequest.name
        dao.messageService.memPool.put(msg.signedMessageData.hash, ChannelMessageMetadata(msg)).unsafeRunSync()
        dao.threadSafeMessageMemPool.put(Seq(msg), overrideLimit = true)
        val semaphore = new Semaphore(1)
        dao.threadSafeMessageMemPool.activeChannels(genesisHashChannelId) = semaphore
        semaphore.acquire()
        Future {
          var retries = 0
          var metadata: Option[ChannelMetadata] = None
          while (retries < 30 && metadata.isEmpty) {
            retries += 1
            logger.debug(s"Polling genesis creation attempt $retries for $genesisHashChannelId")
            Thread.sleep(1000)
            metadata = dao.channelService.lookup(genesisHashChannelId).unsafeRunSync()
          }
          val response =
            if (metadata.isEmpty) "Timeout awaiting block acceptance"
            else {
              "Success"
            }
          ChannelOpenResponse(response, genesisHashChannelId)
        }(ConstellationExecutionContext.bounded)
      }
  }

  def createMessages(
    channelSendRequest: ChannelSendRequest
  )(implicit dao: DAO): Future[ChannelSendResponse] =
    dao.messageService.memPool
      .lookup(channelSendRequest.channelId)
      .unsafeRunSync()
      .map { previousMessage =>
        val previous = previousMessage.channelMessage.signedMessageData.hash

        val messages: Seq[ChannelMessage] = channelSendRequest.messages
          .foldLeft(previous -> Seq[ChannelMessage]()) {
            case ((prvHash, signedMessages), nextMessage) =>
              val nextSigned = create(nextMessage, prvHash, channelSendRequest.channelId)(dao.keyPair)
              nextSigned.signedMessageData.hash -> (signedMessages :+ nextSigned)
          }
          ._2

        dao.threadSafeMessageMemPool.put(messages, overrideLimit = true)
        messages.foreach(cm => {
          dao.messageService.memPool.put(cm.signedMessageData.hash, ChannelMessageMetadata(cm)).unsafeRunSync()
        })
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
