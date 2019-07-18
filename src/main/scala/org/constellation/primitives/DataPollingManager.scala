package org.constellation.primitives

import com.typesafe.scalalogging.StrictLogging
import constellation.futureTryWithTimeoutMetric
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.extension.TransitService
import org.constellation.util.Periodic

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

//noinspection ScalaStyle
class DataPollingManager(periodSeconds: Int = 60)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("DataPollingManager", periodSeconds)
    with StrictLogging {

  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.edge

  private val transitService = new TransitService()

  private val channelName = "transit"

  @volatile private var channelId: String = _

  ChannelMessage.createGenesis(ChannelOpen(channelName)).foreach { resp =>
    channelId = resp.genesisHash
  }

  // Need a better way to manage these, but hardcode for now.
  private val bartTransitUrl = "https://api.bart.gov/gtfsrt/tripupdate.aspx"

  private def execute(channelId: String) =
    futureTryWithTimeoutMetric(
      {
        val latest = transitService.pollJson(bartTransitUrl)
        latest.foreach { msg =>
          logger.info(s"Polled message from transit service, inserted into channel $channelId")
          ChannelMessage.createMessages(ChannelSendRequest(channelId, Seq(msg)))
        }
      },
      "dataPolling",
      60, {
        dao.metrics.incrementMetric("dataPollingFailure")
      }
    )

  override def trigger(): Future[Try[Unit]] =
    if (channelId != null) {
      execute(channelId)
    } else Future.successful(Try(Unit))
}
