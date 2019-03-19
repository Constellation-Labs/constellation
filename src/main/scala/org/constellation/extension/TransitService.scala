package org.constellation.extension

import com.google.transit.realtime.gtfs_realtime.FeedMessage
import com.typesafe.scalalogging.StrictLogging
import org.constellation.util.APIClient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class TransitService extends StrictLogging {

  def poll(feedUrl: String) = {

    val w = new java.net.URL(feedUrl)

    val r = (w.getHost, w.getPath)

    val apiClient = APIClient(w.getHost, 80)

    val respF = apiClient.getBytes(w.getPath)

    val resp = Await.result(respF, 60 seconds)

    val message = FeedMessage.parseFrom(resp.unsafeBody)

    message

  }


}

object TransitService extends StrictLogging {
  def main(args: Array[String]): Unit = {
    val t = new TransitService()
    val q = t.poll("http://api.bart.gov/gtfsrt/tripupdate.aspx")

    logger.info(q.toString)
  }
}
