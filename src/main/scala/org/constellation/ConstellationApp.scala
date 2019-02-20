package org.constellation
import com.softwaremill.sttp.Response
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives._
import org.constellation.util.APIClient

import scala.concurrent.ExecutionContext

class ConstellationApp(
                        val clientApi: APIClient
                      )(implicit val ec: ExecutionContext) {

  val channelIdToChannel = scala.collection.mutable.HashMap[String, Channel]()
  def registerChannels(chaMsg: Channel) = channelIdToChannel.update(chaMsg.channelId, chaMsg)

  //todo add auth for redeploy
  def deploy(
              schemaStr: String,
              channelName: String = s"channel_${channelIdToChannel.keys.size + 1}"
            )(implicit ec: ExecutionContext) = {
    val response = clientApi.postNonBlocking[Some[GenesisResponse]]("channel/open", ChannelOpenRequest(channelName, jsonSchema = Some(schemaStr)))
    response.map { resp =>
      val channelMsg = resp.map(_.channelMsg).map { msg =>
        Channel(msg.signedMessageData.hash, channelName, msg.signedMessageData.data)
      }
      channelMsg.foreach(registerChannels)
      channelMsg
    }
  }

  def broadcast[T <: ChannelRequest](messages: Seq[T])(implicit ec: ExecutionContext) = {
    val msgType = messages.map(_.channelName).head//todo handle multiple message types, or throw error
    val serializedMessages = messages.map(_.json)
    clientApi.postNonBlocking[Seq[ChannelMessage]](
      "channel/send",
      ChannelSendRequest(msgType, serializedMessages)
      )
  }

}

case class Channel(channelId: String, channelName: String, genesisMsgChannelData: ChannelMessageData)

