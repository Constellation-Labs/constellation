package org.constellation.util

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.pattern.ask
import akka.util.Timeout
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.DAO
import org.constellation.consensus.{GetMemPool, MemPool}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.json4s.native.Serialization


case class NodeStateInfo(nodeState: NodeState)

trait CommonEndpoints extends Json4sSupport {

  implicit val serialization: Serialization.type

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]

  implicit val _timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  val dao: DAO

  val commonEndpoints: Route = get {
    path("health") {
      complete(StatusCodes.OK)
    } ~
      path("id") {
        complete(dao.id)
      } ~
      path("tips") {
        complete(dao.threadSafeTipService.tips)
      } ~
      path("info") {
        complete(dao.threadSafeTipService.getSnapshotInfo)
      } ~
      path("snapshot" / Segment) {s =>
        complete(dao.dbActor.getSnapshot(s))
      } ~
      path("genesis") {
        complete(dao.genesisObservation)
      } ~
      pathPrefix("address" / Segment) { a =>
        complete(dao.dbActor.getAddressCacheData(a))
      } ~
      pathPrefix("balance" / Segment) { a =>
        complete(dao.dbActor.getAddressCacheData(a).map{_.balanceByLatestSnapshot})
      } ~
    path("state") {
      complete(NodeStateInfo(dao.nodeState))
    }
  }
}
