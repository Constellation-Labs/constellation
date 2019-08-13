package org.constellation

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

trait ConfigEndpoints {

  implicit val dao: DAO

  def configEndpoints: Route = configPostEndpoints ~ configDeleteEndpoints

  private val configPostEndpoints: Route = post {
    path("random") {
      dao.enableRandomTransactions()
      complete(StatusCodes.OK)
    } ~
      path("timeout") {
        dao.enableSimulateEndpointTimeout()
        complete(StatusCodes.OK)
      }
  }

  private val configDeleteEndpoints: Route = delete {
    path("random") {
      dao.disableRandomTransactions()
      complete(StatusCodes.OK)
    } ~
      path("timeout") {
        dao.disableSimulateEndpointTimeout()
        complete(StatusCodes.OK)
      }
  }
}
