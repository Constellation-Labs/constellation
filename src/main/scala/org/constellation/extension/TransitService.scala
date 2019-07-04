package org.constellation.extension

import com.google.transit.realtime.gtfs_realtime.FeedMessage
import com.typesafe.scalalogging.StrictLogging
import org.constellation.ConstellationExecutionContext
import org.constellation.util.APIClientBase
import org.json4s.native.Serialization
import org.json4s.{Extraction, Formats}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class TransitService extends StrictLogging {

  implicit val formats: Formats = org.json4s.DefaultFormats
  val httpPort = 80

  def poll(feedUrl: String): Future[FeedMessage] = {

    val w = new java.net.URL(feedUrl)
    val apiClient = APIClientBase(w.getHost, httpPort)(ConstellationExecutionContext.global)
    val respF = apiClient.getBytes(w.getPath)
    val message = respF.map { r =>
      FeedMessage.parseFrom(r.unsafeBody)
    }(ConstellationExecutionContext.global)

    message
  }

  def pollJson(feedUrl: String): Future[String] = {
    val msgF = poll(feedUrl)
    msgF.map { msg =>
      Serialization.write(Extraction.decompose(msg))
    }(ConstellationExecutionContext.global)
  }

}

object TransitService extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val t = new TransitService()
    val asJsonString =
      Await.result(t.pollJson("https://api.bart.gov/gtfsrt/tripupdate.aspx"), 60 seconds)
  }
}
