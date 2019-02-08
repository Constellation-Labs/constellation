package org.constellation
import com.softwaremill.sttp.Response
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives._
import org.constellation.util.APIClient

import scala.concurrent.{ExecutionContext, Future}

class ConstellationApp(val constellationNode: ConstellationNode) {

  val clientApi = constellationNode.getAPIClientForNode(constellationNode)
  val channels = scala.collection.mutable.HashMap[String, Channel]()

  def deploy(schemaStr: String, channelId: String = KeyUtils.makeKeyPair().getPublic.hex)(implicit executionContext: ExecutionContext) = {
    val response: Future[Some[GenesisResponse]] =
      clientApi.postNonBlocking("channel/open", ChannelOpenRequest(channelId, jsonSchema = Some(schemaStr)))
    val genesisMessageStr: Future[String] = response.map(resp => resp.value.genesisMessageStr)
    val channelMsg = response.map(resp => resp.value.channelMsg)
    for {
    gnenesisMsg <- genesisMessageStr
    channelMsg <- channelMsg
    } yield {
      channels.update(gnenesisMsg, Channel(channelMsg.signedMessageData.data, gnenesisMsg))
    }
  }

  def broadcast[T <% ChannelMessageData](messages: Seq[T]) = {
    val msgTypes = messages.map(_.channelId).head//channelId is just genesis hash, equivalent to channelID
    //todo handle multiple message types, or throw error
    val serializedMessages = messages.map(_.json)
    val reqs = clientApi.postNonBlocking[Seq[ChannelMessage]](
      "channel/send",
      ChannelSendRequest(msgTypes, serializedMessages)
    )
  }


}

case class Channel(genesisMsg: ChannelMessageData, channelId: String)

