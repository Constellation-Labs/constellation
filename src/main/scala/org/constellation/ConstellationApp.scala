package org.constellation
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.primitives._
import org.constellation.util.APIClient

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ConstellationApp(
  val clientApi: APIClient
)(implicit val ec: ExecutionContext)
    extends StrictLogging {

  val channelNameToId = scala.collection.mutable.HashMap[String, Channel]()
  def registerChannels(chaMsg: Channel) = channelNameToId.update(chaMsg.name, chaMsg)

  //todo add auth for redeploy
  def deploy(
    schemaStr: String,
    channelName: String = s"test_channel_${channelNameToId.keys.size + 1}"
  )(implicit ec: ExecutionContext) = {
    val response = clientApi.postNonBlocking[Option[ChannelOpenResponse]](
      "channel/open",
      ChannelOpen(channelName, jsonSchema = Some(schemaStr)),
      timeout = 120.seconds
    )
    response.map { resp =>
      val channelMsg = resp.map { msg =>
        assert(msg.errorMessage == "Success")
        Channel(msg.genesisHash, channelName, msg)
      }
      channelMsg.foreach(registerChannels)
      channelMsg
    }
  }

  def broadcast[T](messages: Seq[T], channelId: String)(implicit ec: ExecutionContext) = {
    val serializedMessages = messages.map(_.json)
    logger.info(s"messages: ${messages} channelId: ${ channelId}")
    clientApi.postNonBlocking[ChannelSendResponse](
      "channel/send",
      ChannelSendRequest(channelId, serializedMessages),
      timeout = 120.seconds
    )
  }
}

case class Channel(channelId: String, name: String, channelOpenRequest: ChannelOpenResponse)
