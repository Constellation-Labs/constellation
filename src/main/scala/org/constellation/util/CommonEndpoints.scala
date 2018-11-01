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
import org.constellation.consensus.StoredSnapshot
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.serializer.KryoSerializer
import org.json4s.native.Serialization

import scala.concurrent.Future


case class NodeStateInfo(nodeState: NodeState)

trait CommonEndpoints extends Json4sSupport {

  implicit val serialization: Serialization.type

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]

  implicit val _timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  implicit val dao: DAO

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
    path("snapshotHashes") {
      complete(dao.snapshotPath.list.toSeq.map{_.name})
    } ~
      path("info") {
        val info = dao.threadSafeTipService.getSnapshotInfo

        complete(info.copy(acceptedCBSinceSnapshotCache = info.acceptedCBSinceSnapshot.flatMap{dao.checkpointService.get}))
      } ~
      path("snapshot" / Segment) {s =>
        complete(dao.dbActor.getSnapshot(s))
      } ~
      path("storedSnapshot" / Segment) {s =>
        onComplete{
          Future{
            import better.files._
            tryWithMetric({
              KryoSerializer.deserializeCast[StoredSnapshot](File(dao.snapshotPath, s).byteArray)
            },
              "readSnapshot"
            )
          }(dao.edgeExecutionContext)
        } {
          res =>
            complete(res.toOption.flatMap{_.toOption})
        }

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
    } ~
    path("peers") {
      complete(dao.peerInfo.map{_._2.peerMetadata}.toSeq)
    } ~
    path("transaction" / Segment) {
      h =>
        complete(dao.transactionService.get(h))
    } ~
    path("checkpoint" / Segment) { h => complete(dao.checkpointService.get(h))}

  }
}
