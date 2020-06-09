package org.constellation.infrastructure.endpoints

import cats.effect.{Concurrent, IO}
import cats.implicits._
import io.circe.syntax._
import io.circe.Encoder
import io.circe.generic.semiauto._
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.redownload.RedownloadService
import org.constellation.domain.redownload.RedownloadService.LatestMajorityHeight
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeState
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.SnapshotService
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import scala.collection.SortedMap

import Id._
import RedownloadService._
import LatestMajorityHeight._

class SnapshotEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  implicit val smEncoder: Encoder[SortedMap[Id, Double]] =
    Encoder.encodeMap[Id, Double].contramap[SortedMap[Id, Double]](_.toMap)

  def peerEndpoints(
    nodeId: Id,
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    snapshotService: SnapshotService[F],
    cluster: Cluster[F],
    redownloadService: RedownloadService[F]
  ) =
    getStoredSnapshotsEndpoint(snapshotStorage) <+>
      getStoredSnapshotByHash(snapshotStorage) <+>
      getCreatedSnapshotsEndpoint(redownloadService) <+>
      getAcceptedSnapshotsEndpoint(redownloadService) <+>
      getPeerProposals(redownloadService) <+>
      getNextSnapshotHeight(nodeId, snapshotService) <+>
      getSnapshotInfo(snapshotService, cluster) <+>
      getSnapshotInfoByHash(snapshotInfoStorage) <+>
      getLatestMajorityHeight(redownloadService)

  def ownerEndpoints(
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    redownloadService: RedownloadService[F]
  ) =
    getStoredSnapshotsEndpoint(snapshotStorage) <+>
      getStoredSnapshotByHash(snapshotStorage) <+>
      getCreatedSnapshotsEndpoint(redownloadService) <+>
      getAcceptedSnapshotsEndpoint(redownloadService) <+>
      getPeerProposals(redownloadService) <+>
      getLatestMajorityHeight(redownloadService)

  private def getStoredSnapshotsEndpoint(snapshotStorage: LocalFileStorage[F, StoredSnapshot]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "stored" =>
        snapshotStorage.list().rethrowT.map(_.asJson).flatMap(Ok(_))
    }

  private def getStoredSnapshotByHash(snapshotStorage: LocalFileStorage[F, StoredSnapshot]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "stored" / hash => {
        val getSnapshot = for {
          exists <- snapshotStorage.exists(hash)
          bytes <- if (exists) {
            snapshotStorage.readBytes(hash).rethrowT.map(_.some)
          } else none[Array[Byte]].pure[F]
        } yield bytes

        getSnapshot.flatMap {
          case Some(snapshot) => Ok(snapshot)
          case None           => NotFound()
        }
      }
    }

  private def getCreatedSnapshotsEndpoint(redownloadService: RedownloadService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "created" =>
        redownloadService.getCreatedSnapshots().map(_.asJson).flatMap(Ok(_))
    }

  private def getAcceptedSnapshotsEndpoint(redownloadService: RedownloadService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "accepted" =>
        redownloadService.getAcceptedSnapshots().map(_.asJson).flatMap(Ok(_))
    }

  private def getPeerProposals(redownloadService: RedownloadService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "peer" / id / "snapshot" / "created" =>
        redownloadService.getPeerProposals().map(_.get(Id(id))).map(_.asJson).flatMap(Ok(_))
    }

  implicit val idLongEncoder: Encoder[(Id, Long)] = deriveEncoder

  private def getNextSnapshotHeight(nodeId: Id, snapshotService: SnapshotService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "nextHeight" =>
        snapshotService.getNextHeightInterval.map((nodeId, _)).map(_.asJson).flatMap(Ok(_))
    }

  private def getSnapshotInfo(snapshotService: SnapshotService[F], cluster: Cluster[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "info" => {
        val getSnapshotInfo = snapshotService.getSnapshotInfoWithFullData.flatMap { si =>
          F.delay(KryoSerializer.serialize[SnapshotInfo](si))
        }

        val result = cluster.getNodeState
          .map(NodeState.canActAsRedownloadSource)
          .ifM(
            getSnapshotInfo.map(_.some),
            none[Array[Byte]].pure[F]
          )

        result.flatMap {
          case Some(snapshotInfo) => Ok(snapshotInfo)
          case None               => ServiceUnavailable()
        }
      }
    }

  private def getSnapshotInfoByHash(snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "snapshot" / "info" / hash => {
        val getSnapshotInfo = for {
          exists <- snapshotInfoStorage.exists(hash)
          bytes <- if (exists) {
            snapshotInfoStorage.readBytes(hash).rethrowT.map(_.some)
          } else none[Array[Byte]].pure[F]
        } yield bytes

        getSnapshotInfo.flatMap {
          case Some(snapshotInfo) => Ok(snapshotInfo)
          case None               => NotFound()
        }
      }
    }

  private def getLatestMajorityHeight(redownloadService: RedownloadService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "latestMajorityHeight" =>
      (redownloadService.lowestMajorityHeight, redownloadService.latestMajorityHeight)
        .mapN(LatestMajorityHeight(_, _))
        .map(_.asJson)
        .flatMap(Ok(_))
  }
}

object SnapshotEndpoints {

  def peerEndpoints[F[_]: Concurrent](
    nodeId: Id,
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    snapshotService: SnapshotService[F],
    cluster: Cluster[F],
    redownloadService: RedownloadService[F]
  ): HttpRoutes[F] =
    new SnapshotEndpoints[F]()
      .peerEndpoints(nodeId, snapshotStorage, snapshotInfoStorage, snapshotService, cluster, redownloadService)

  def ownerEndpoints[F[_]: Concurrent](
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    redownloadService: RedownloadService[F]
  ): HttpRoutes[F] =
    new SnapshotEndpoints[F]()
      .ownerEndpoints(snapshotStorage, redownloadService)
}
