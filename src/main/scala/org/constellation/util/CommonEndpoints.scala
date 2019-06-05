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
import org.constellation.primitives.Schema.NodeType
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.serializer.KryoSerializer
import org.json4s.native.Serialization

import scala.concurrent.Future

case class NodeStateInfo(
  nodeState: NodeState,
  addresses: Seq[String] = Seq(),
  nodeType: NodeType = NodeType.Full
) // TODO: Refactor, addresses temp for testing

trait CommonEndpoints extends Json4sSupport {

  implicit val serialization: Serialization.type

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]

  implicit val _timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  implicit val dao: DAO

  val commonEndpoints: Route = get {
    path("health") {
      val metricFailure = HealthChecker.checkLocalMetrics(dao.metrics.getMetrics, dao.id.short)
      metricFailure match {
        case Left(value) => failWith(value)
        case Right(_) => complete(StatusCodes.OK)
      }
    } ~
      path("id") {
        complete(dao.id)
      } ~
      path("tips") {
        complete(dao.concurrentTipService.toMap)
      } ~
      path("heights") {
        val maybeHeights = dao.concurrentTipService.toMap.flatMap {
          case (k, _) => dao.checkpointService.lookup(k).unsafeRunSync().flatMap { _.height }
        }.toSeq
        complete(maybeHeights)
      } ~
      path("snapshotHashes") {
        complete(Snapshot.snapshotHashes())
      } ~
      path("info") {
        val info = dao.threadSafeSnapshotService.getSnapshotInfo
        val res =
          KryoSerializer.serializeAnyRef(
            info.copy(acceptedCBSinceSnapshotCache = info.acceptedCBSinceSnapshot.flatMap {
              dao.checkpointService.fullData(_).unsafeRunSync()
            })
          )
        complete(res)
      } ~
/*      path("snapshot" / Segment) {s =>
        complete(dao.dbActor.getSnapshot(s))
      } ~*/
      path("storedSnapshot" / Segment) { s =>
        onComplete {
          Future {
            Snapshot.loadSnapshotBytes(s)
          }(dao.edgeExecutionContext)
        } { res =>
          val byteArray = res.toOption.flatMap { _.toOption }.getOrElse(Array[Byte]())

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
        complete(dao.addressService.lookup(a).unsafeRunSync())
      } ~
      pathPrefix("balance" / Segment) { a =>
        complete(dao.addressService.lookup(a).unsafeRunSync().map { _.balanceByLatestSnapshot })
      } ~
      path("state") {
        complete(NodeStateInfo(dao.nodeState, dao.addresses, dao.nodeType))
      } ~
      path("peers") {
        val peers = dao.peerInfo.map(_.map(_._2.peerMetadata).toSeq)
        onComplete(peers.unsafeToFuture) { a =>
          complete(a.toOption.getOrElse(Seq()))
        }
      } ~
      path("transaction" / Segment) { h =>
        complete(dao.transactionService.lookup(h).unsafeRunSync())
      } ~
      path("message" / Segment) { h =>
        complete(dao.messageService.memPool.lookup(h).unsafeRunSync())
      } ~
      path("checkpoint" / Segment) { h =>
        complete(dao.checkpointService.lookup(h).unsafeRunSync())
      }

  }
}
