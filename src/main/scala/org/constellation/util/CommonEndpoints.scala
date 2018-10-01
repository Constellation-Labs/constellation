package org.constellation.util

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.Data
import org.json4s.native
import org.json4s.native.Serialization
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import constellation._

trait CommonEndpoints extends Json4sSupport {

  implicit val serialization: Serialization.type

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]

  val dao: Data

  val commonEndpoints: Route = get {
    path("health") {
      complete(StatusCodes.OK)
    } ~
      path("id") {
        complete(dao.id)
      } ~
    path("tips") {
      complete(dao.checkpointMemPoolThresholdMet.map{_._2._1}.toSeq)
    }
  }
}
