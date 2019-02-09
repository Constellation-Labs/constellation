package org.constellation
import com.softwaremill.sttp.Response
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives._
import org.constellation.util.APIClient

import scala.concurrent.{ExecutionContext, Future}

class ConstellationApp(
                        val clientApi: APIClient
                      ) {

  val channels = scala.collection.mutable.HashMap[String, Channel]()

  def deploy(schemaStr: String, channelId: String = KeyUtils.makeKeyPair().getPublic.hex) = {
//    val response: Future[Some[GenesisResponse]] =
//      clientApi.postNonBlocking("channel/open", ChannelOpenRequest(channelId, jsonSchema = Some(schemaStr)))
//    val genesisMessageStr: Future[String] = response.map(resp => resp.value.genesisMessageStr)
//    val channelMsg = response.map(resp => resp.value.channelMsg)
//    for {
//    gnenesisMsg <- genesisMessageStr
//    channelMsg <- channelMsg
//    } yield {
//      channels.update(gnenesisMsg, Channel(channelMsg.signedMessageData.data, gnenesisMsg))
//    }
    clientApi.postSync("channel/open", ChannelOpenRequest(channelId, jsonSchema = Some(schemaStr)))

  }

  def broadcast(messages: Seq[ChannelSendRequest]) = {
    val msgTypes = messages.map(_.channelId).head//channelId is just genesis hash, equivalent to channelID
    //todo handle multiple message types, or throw error
    val serializedMessages = messages.map(_.json)
    clientApi.postBlocking[Seq[ChannelMessage]](
      "channel/send",
      ChannelSendRequest(msgTypes, serializedMessages)
      )
  }


}

case class Channel(genesisMsg: ChannelMessageData, channelId: String)

