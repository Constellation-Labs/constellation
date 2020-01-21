package org.constellation.util

import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.util.ByteString
import cats.effect.IO
import cats.implicits._
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.serializer.KryoSerializer
import org.json4s.native.Serialization

import scala.util.{Failure, Success}

case class NodeStateInfo(
  nodeState: NodeState,
  addresses: Seq[String] = Seq(),
  nodeType: NodeType = NodeType.Full
) // TODO: Refactor, addresses temp for testing

trait CommonEndpoints extends Json4sSupport {

  implicit val serialization: Serialization.type

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]

  implicit val dao: DAO

  val commonEndpoints: Route = get {
    path("health") {
      val metricFailure = HealthChecker.checkLocalMetrics(dao.metrics.getMetrics, dao.id.short)
      metricFailure match {
        case Left(value) => failWith(value)
        case Right(_)    => complete(StatusCodes.OK)
      }
    } ~
      path("id") {
        complete(dao.id)
      } ~
      path("tips") {
        APIDirective.handle(dao.concurrentTipService.toMap)(complete(_))
      } ~
      path("heights") {
        val calculateHeights = for {
          tips <- dao.concurrentTipService.toMap
          maybeHeights <- tips.toList.traverse(t => dao.checkpointService.lookup(t._1))
        } yield maybeHeights.flatMap(_.flatMap(_.height))

        APIDirective.handle(calculateHeights)(complete(_))
      } ~
      path("heights" / "min") {
        APIDirective.handle(dao.concurrentTipService.getMinTipHeight(None).map((dao.id, _)))(complete(_))
      } ~
      path("snapshotHashes") {
        APIDirective.handle(dao.snapshotStorage.getSnapshotHashes)(complete(_))
      } ~
      path("snapshot" / "recent") {
        APIDirective.handle(
          dao.snapshotBroadcastService.getRecentSnapshots
        )(complete(_))
      } ~
      path("snapshot" / "reputation") {
        APIDirective.handle(
          dao.snapshotBroadcastService.getRecentSnapshots
        )(complete(_))
      } ~
      path("snapshot" / "nextHeight") {
        APIDirective.handle(
          dao.snapshotService.getNextHeightInterval.map((dao.id, _))
        )(complete(_))
      } ~
      path("info") {
        val getInfo = dao.snapshotService.getSnapshotInfo.flatMap { info =>
          info.acceptedCBSinceSnapshot.toList.traverse {
            dao.checkpointService.fullData(_)
          }.map(cbs => KryoSerializer.serializeAnyRef(info.copy(acceptedCBSinceSnapshotCache = cbs.flatten)).some)
        }

        APIDirective.handle(
          dao.cluster.getNodeState.map(NodeState.canActAsDownloadSource).ifM(getInfo, IO.pure(none[Array[Byte]]))
        )(complete(_))
      } ~
      path("storedSnapshot" / Segment) { s =>
        val getSnapshot = for {
          exists <- dao.snapshotService.exists(s)
          bytes <- if (exists) {
            IO(Snapshot.loadSnapshotBytes(s).toOption)
          } else None.pure[IO]
        } yield bytes

        APIDirective.onHandle(getSnapshot) { res =>
          val httpResponse: HttpResponse = res match {
            case Failure(_) =>
              HttpResponse(StatusCodes.NotFound)
            case Success(None) =>
              HttpResponse(StatusCodes.NotFound)
            case Success(Some(bytes)) =>
              HttpResponse(
                entity = HttpEntity.Strict(MediaTypes.`application/octet-stream`, ByteString(bytes))
              )
          }

          complete(httpResponse)
        }

      } ~
      path("genesis") {
        complete(dao.genesisObservation)
      } ~
      pathPrefix("address" / Segment) { a =>
        APIDirective.handle(dao.addressService.lookup(a))(complete(_))
      } ~
      pathPrefix("balance" / Segment) { a =>
        APIDirective.handle(dao.addressService.lookup(a).map(_.map(_.balanceByLatestSnapshot)))(complete(_))
      } ~
      path("state") {
        APIDirective.handle(dao.cluster.getNodeState)(res => complete(NodeStateInfo(res, dao.addresses, dao.nodeType)))
      } ~
      path("peers") {
        APIDirective.handle(dao.peerInfo.map(_.map(_._2.peerMetadata).toSeq))(complete(_))
      } ~
      path("transaction" / Segment) { h =>
        APIDirective.handle(dao.transactionService.lookup(h))(complete(_))
      } ~
      path("message" / Segment) { h =>
        APIDirective.handle(dao.messageService.memPool.lookup(h))(complete(_))
      } ~
      path("checkpoint" / Segment) { h =>
        APIDirective.handle(dao.checkpointService.fullData(h))(complete(_))
      } ~
      path("soe" / Segment) { h =>
        APIDirective.handle(dao.soeService.lookup(h))(complete(_))
      } ~
      path("observation" / Segment) { h =>
        APIDirective.handle(dao.observationService.lookup(h))(complete(_))
      }
  }

  val batchEndpoints: Route = post {
    pathPrefix("batch") {
      path("transactions") {
        entity(as[List[String]]) { ids =>
          dao.metrics.incrementMetric(Metrics.batchTransactionsEndpoint)

          APIDirective.handle(
            ids.traverse(id => dao.transactionService.lookup(id).map((id, _))).map(_.filter(_._2.isDefined))
          )(complete(_))
        }
      } ~
        path("observations") {
          entity(as[List[String]]) { ids =>
            dao.metrics.incrementMetric(Metrics.batchObservationsEndpoint)

            APIDirective.handle(
              ids.traverse(id => dao.observationService.lookup(id).map((id, _))).map(_.filter(_._2.isDefined))
            )(complete(_))
          }
        }
    }
  }
}
