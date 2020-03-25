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
import org.constellation.domain.redownload.RedownloadService.LatestMajorityHeight
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.primitives.Schema.NodeType.NodeType
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
      // TODO: Move to PeerAPI - Not used publicly
      path("tips") {
        APIDirective.handle(dao.concurrentTipService.toMap)(complete(_))
      } ~
      // TODO: Move to PeerAPI - Not used publicly
      path("heights") {
        val calculateHeights = for {
          tips <- dao.concurrentTipService.toMap
          maybeHeights <- tips.toList.traverse(t => dao.checkpointService.lookup(t._1))
        } yield maybeHeights.flatMap(_.flatMap(_.height))

        APIDirective.handle(calculateHeights)(complete(_))
      } ~
      // TODO: Move to PeerAPI - Used in HealthChecker so could be moved to PeerAPI as we have it already in UI.
      path("heights" / "min") {
        APIDirective.handle(dao.concurrentTipService.getMinTipHeight(None).map((dao.id, _)))(complete(_))
      } ~
      // TODO: Used in PeerAPI but also:
      // TODO: Move to different port - Should be probably accessible by owner only.
      pathPrefix("snapshot") {
        // TODO: Used in PeerAPI but also:
        // TODO: Move to different port - Should be probably accessible by owner only.
        path("stored") {
          val storedSnapshots = dao.snapshotStorage.list().rethrowT

          APIDirective.handle(storedSnapshots)(complete(_))
        } ~
          // TODO: Used in PeerAPI but also:
          // TODO: Move to different port - Should be probably accessible by owner only.
          path("created") {
            val snapshots = dao.redownloadService.getCreatedSnapshots()

            APIDirective.handle(snapshots)(complete(_))
          } ~
          // TODO: Used in PeerAPI but also:
          // TODO: Move to different port - Should be probably accessible by owner only.
          path("accepted") {
            val snapshots = dao.redownloadService.getAcceptedSnapshots()

            APIDirective.handle(snapshots)(complete(_))
          } ~
          // TODO: Used in PeerAPI but also:
          // TODO: Move to different port - Should be probably accessible by owner only.
          path("nextHeight") {
            APIDirective.handle(
              dao.snapshotService.getNextHeightInterval.map((dao.id, _))
            )(complete(_))
          } ~
          // TODO: Used in PeerAPI but also:
          // TODO: Move to different port - Should be probably accessible by owner only.
          path("info") {
            val getSnapshotInfo = dao.snapshotService.getSnapshotInfoWithFullData
              .map(KryoSerializer.serialize[SnapshotInfo])

            val result = dao.cluster.getNodeState
              .map(NodeState.canActAsRedownloadSource)
              .ifM(
                getSnapshotInfo.map(_.some),
                None.pure[IO]
              )

            APIDirective.onHandle(result) { res =>
              val httpResponse: HttpResponse = res match {
                case Failure(_) => HttpResponse(StatusCodes.ServiceUnavailable)
                case Success(Some(snapshotInfo)) =>
                  HttpResponse(
                    entity = HttpEntity.Strict(MediaTypes.`application/octet-stream`, ByteString(snapshotInfo))
                  )
                case Success(None) => HttpResponse(StatusCodes.ServiceUnavailable)
              }

              complete(httpResponse)
            }
          } ~
          // TODO: Used in PeerAPI but also:
          // TODO: Move to different port - Should be probably accessible by owner only.
          path("info" / Segment) { s =>
            val getSnapshotInfo = for {
              exists <- dao.snapshotInfoStorage.exists(s)
              bytes <- if (exists) {
                dao.snapshotInfoStorage.readBytes(s).rethrowT.map(Some(_))
              } else none[Array[Byte]].pure[IO]
            } yield bytes

            APIDirective.onHandle(getSnapshotInfo) { res =>
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
          }
      } ~
      // TODO: Make it snapshot/stored/:hash and move under pathPrefix("snapshot")
      // TODO: Used in PeerAPI but also:
      // TODO: Move to different port - Should be probably accessible by owner only.
      path("storedSnapshot" / Segment) { s =>
        val getSnapshot = for {
          exists <- dao.snapshotStorage.exists(s)
          bytes <- if (exists) {
            dao.snapshotStorage.readBytes(s).value.flatMap(IO.fromEither).map(Some(_))
          } else none[Array[Byte]].pure[IO]
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
      // Move to PeerAPI - Used in Simulation so could be moved to PeerAPI as we have it in UI.
      path("genesis") {
        complete(dao.genesisObservation)
      } ~
      pathPrefix("address" / Segment) { a =>
        APIDirective.handle(dao.addressService.lookup(a))(complete(_))
      } ~
      // Move to PeerAPI - Used in Cluster so could be moved to PeerAPI as we have it in UI.
      path("state") {
        APIDirective.handle(dao.cluster.getNodeState)(res => complete(NodeStateInfo(res, dao.addresses, dao.nodeType)))
      } ~
      // Move to PeerAPI - Used in Redownload so could be moved to PeerAPI
      path("latestMajorityHeight") {
        val height = (dao.redownloadService.lowestMajorityHeight, dao.redownloadService.latestMajorityHeight)
          .mapN(LatestMajorityHeight)

        APIDirective.handle(height)(complete(_))
      } ~
      // Move to PeerAPI - Used in PeerDiscovery so could be moved to PeerAPI
      path("peers") {
        APIDirective.handle(dao.peerInfo.map(_.map(_._2.peerMetadata).toSeq))(complete(_))
      } ~
      // Move to PeerAPI - Used in DataResolver so could be moved to PeerAPI
      path("transaction" / Segment) { h =>
        APIDirective.handle(dao.transactionService.lookup(h))(complete(_))
      } ~
      // Move to PeerAPI - Used in DataResolver so could be moved to PeerAPI
      path("checkpoint" / Segment) { h =>
        APIDirective.handle(dao.checkpointService.fullData(h))(complete(_))
      } ~
      // Move to PeerAPI - Used in DataResolver so could be moved to PeerAPI
      path("soe" / Segment) { h =>
        APIDirective.handle(dao.soeService.lookup(h))(complete(_))
      } ~
      // Move to PeerAPI - Used in DataResolver so could be moved to PeerAPI
      path("observation" / Segment) { h =>
        APIDirective.handle(dao.observationService.lookup(h))(complete(_))
      }
  }
}
