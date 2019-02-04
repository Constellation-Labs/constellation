package org.constellation.util

import java.util.concurrent.TimeUnit
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.util.{ByteString, Timeout}

import constellation._

import de.heikoseeberger.akkahttpjson4s.Json4sSupport

import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.serializer.KryoSerializer

import org.json4s.native.Serialization
import scala.concurrent.Future

/** Documentation. */
case class NodeStateInfo(nodeState: NodeState)

/** Documentation. */
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
      path("heights") {
        val maybeHeights = dao.threadSafeTipService.tips
          .flatMap { case (k, v) => dao.checkpointService.get(k).flatMap {_.height}}.toSeq
        complete(maybeHeights)
      } ~
    path("snapshotHashes") {
      complete(Snapshot.snapshotHashes())
    } ~
      path("info") {
        val info = dao.threadSafeTipService.getSnapshotInfo
        val res =
          KryoSerializer.serializeAnyRef(
            info.copy(acceptedCBSinceSnapshotCache = info.acceptedCBSinceSnapshot.flatMap{dao.checkpointService.get})
          )
        complete(res)
      } ~
/*      path("snapshot" / Segment) {s =>
        complete(dao.dbActor.getSnapshot(s))
      } ~*/
      path("storedSnapshot" / Segment) {s =>
        onComplete{
          Future{
            Snapshot.loadSnapshotBytes(s)
          }(dao.edgeExecutionContext)
        } {
          res =>
            val byteArray = res.toOption.flatMap {_.toOption}.getOrElse(Array[Byte]())

            val body = ByteString(byteArray)

            val entity = HttpEntity.Strict(MediaTypes.`application/octet-stream`, body)

            val httpResponse = HttpResponse(entity = entity)

            complete(httpResponse)
            //complete(bytes)
        }

      } ~
      path("genesis") {
        complete(dao.genesisObservation)
      } ~
      pathPrefix("address" / Segment) { a =>
        complete(dao.addressService.get(a))
      } ~
      pathPrefix("balance" / Segment) { a =>
        complete(dao.addressService.get(a).map{_.balanceByLatestSnapshot})
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
