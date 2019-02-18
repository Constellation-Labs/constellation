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

  def updateChannels(channelId: String)(chaMsg: Channel) = channelIdToChannel.update(channelId,
    Channel(chaMsg.genesisMsgChannelData.previousMessageDataHash, chaMsg.genesisMsgChannelData)
  )

  def deploy(schemaStr: String, channelId: String = KeyUtils.makeKeyPair().getPublic.hex)(implicit ec: ExecutionContext) = {
    val response = clientApi.postNonBlocking[Some[GenesisResponse]]("channel/open", ChannelOpenRequest(channelId, jsonSchema = Some(schemaStr)))
    val channelResponse = response.map { resp =>
      val channelMsg = resp.map(_.channelMsg)
      channelMsg.map { msg => Channel(msg.signedMessageData.data.previousMessageDataHash, msg.signedMessageData.data) }
    }
    channelResponse.foreach { chRespOpt => chRespOpt.foreach(updateChannels(channelId)(_)) }
    channelResponse
  }

  def broadcast[T <: ChannelRequest](messages: Seq[T])(implicit ec: ExecutionContext) = {
    val msgType = messages.map(_.channelId).head//todo handle multiple message types, or throw error
    val serializedMessages = messages.map(_.json)
    clientApi.postNonBlocking[Seq[ChannelMessage]](
      "channel/send",
      ChannelSendRequest(msgType, serializedMessages)
      )
  }


}

case class Channel(channelId: String, genesisMsgChannelData: ChannelMessageData)

