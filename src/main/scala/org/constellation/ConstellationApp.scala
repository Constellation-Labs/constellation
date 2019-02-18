package org.constellation
import com.softwaremill.sttp.Response
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives._
import org.constellation.util.APIClient

import scala.concurrent.{ExecutionContext, Future}

class ConstellationApp(
                        val clientApi: APIClient
                      )(implicit val ec: ExecutionContext) {

  val channels = scala.collection.mutable.HashMap[String, Channel]()

  def deploy(schemaStr: String, channelId: String = KeyUtils.makeKeyPair().getPublic.hex)(implicit ec: ExecutionContext) = {
    val response = clientApi.postNonBlocking[Some[GenesisResponse]]("channel/open", ChannelOpenRequest(channelId, jsonSchema = Some(schemaStr)))
    val channelResponse = response.map { resp =>
      val channelMsg = resp.map(_.channelMsg)
      channelMsg.map {
        msg =>
          Channel(msg.signedMessageData.data.channelId, msg.signedMessageData.data)
        }
    }
    channelResponse.foreach { chRespOpt =>
      chRespOpt.foreach(chaMsg =>
        channels.update(chaMsg.channelId,
          Channel(chaMsg.genesisMsgChannelData.previousMessageDataHash, chaMsg.genesisMsgChannelData))
      )
    }
    channelResponse
  }

  def broadcast(messages: Seq[ChannelSendRequest])(implicit ec: ExecutionContext) = {
    val msgTypes = messages.map(_.channelId).head//channelId is just genesis hash, equivalent to channelID
    //todo handle multiple message types, or throw error
    val serializedMessages = messages.map(_.json)
    clientApi.postNonBlocking[Seq[ChannelMessage]](
      "channel/send",
      ChannelSendRequest(msgTypes, serializedMessages)
      )
  }


}

case class Channel(channelId: String, genesisMsgChannelData: ChannelMessageData)

