package org.constellation.primitives

import constellation.futureTryWithTimeoutMetric
import org.constellation.DAO
import org.constellation.extension.TransitService
import org.constellation.util.Periodic

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

//noinspection ScalaStyle
class DataPollingManager(periodSeconds: Int = 60)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("RandomTransactionManager", periodSeconds) {

  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  private val transitService = new TransitService()

  private val channelName = "transit"

  private val channelOpenResponseF = ChannelMessage.createGenesis(ChannelOpen(channelName))

  // Need a better way to manage these, but hardcode for now.
  private val bartTransitUrl = "https://api.bart.gov/gtfsrt/tripupdate.aspx"

  private def execute() = {
    futureTryWithTimeoutMetric({
      val latest = transitService.pollJson(bartTransitUrl)
      latest.foreach { msg =>
        ChannelMessage.createMessages(ChannelSendRequest(channelName, Seq(msg)))
      }
    }, "dataPolling", 60, { dao.metrics.incrementMetric("dataPollingFailure") })
  }

  override def trigger(): Future[Try[Unit]] = {
    if (channelOpenResponseF.isCompleted) {
      execute()
    } else Future.successful(Try(Unit))
  }
}
