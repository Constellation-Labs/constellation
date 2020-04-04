package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.implicits._
import io.circe.{Decoder, Encoder, KeyEncoder}
import io.circe.generic.auto._
import io.circe.syntax._
import org.constellation.domain.observation.ObservationEvent
import org.constellation.domain.trust.TrustData
import org.constellation.p2p.{Cluster, JoinedHeight, PeerUnregister, SetNodeStatus}
import org.constellation.primitives.Schema.NodeState
import org.constellation.schema.Id
import org.constellation.trust.TrustManager
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class ClusterEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(cluster: Cluster[F]) =
    infoEndpoint(cluster)

  def peerEndpoints(cluster: Cluster[F], trustManager: TrustManager[F]) =
    infoEndpoint(cluster) <+>
      setNodeStatusEndpoint(cluster) <+>
      setJoiningHeightEndpoint(cluster) <+>
      deregisterEndpoint(cluster) <+>
      trustEndpoint(trustManager)

  private def infoEndpoint(cluster: Cluster[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "cluster" / "info" =>
        cluster.clusterNodes().map(_.asJson).flatMap(Ok(_))
    }

  private def setNodeStatusEndpoint(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "status" =>
      (for {
        sns <- req.decodeJson[SetNodeStatus]
        _ <- if (sns.nodeStatus == NodeState.Offline) {
          cluster.markOfflinePeer(sns.id)
        } else {
          cluster.setNodeStatus(sns.id, sns.nodeStatus)
        }
      } yield ()) >> Ok()
  }

  private def setJoiningHeightEndpoint(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "joinedHeight" =>
      (for {
        joiningHeight <- req.decodeJson[JoinedHeight]
        _ <- cluster.updateJoinedHeight(joiningHeight)
      } yield ()) >> Ok()
  }

  private def deregisterEndpoint(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "deregister" =>
      (for {
        peerUnregister <- req.decodeJson[PeerUnregister]
        _ <- cluster.deregister(peerUnregister)
      } yield ()) >> Ok()
  }

  implicit val idEncoder: KeyEncoder[Id] = KeyEncoder.encodeKeyString.contramap[Id](_.hex)

  private def trustEndpoint(trustManager: TrustManager[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "trust" =>
      trustManager.getPredictedReputation.flatMap { predicted =>
        if (predicted.isEmpty) trustManager.getStoredReputation.map(TrustData)
        else TrustData(predicted).pure[F]
      }.map(_.asJson).flatMap(Ok(_))
  }
}

object ClusterEndpoints {

  def publicEndpoints[F[_]: Concurrent](cluster: Cluster[F]): HttpRoutes[F] =
    new ClusterEndpoints[F].publicEndpoints(cluster)

  def peerEndpoints[F[_]: Concurrent](cluster: Cluster[F], trustManager: TrustManager[F]): HttpRoutes[F] =
    new ClusterEndpoints[F].peerEndpoints(cluster, trustManager)
}
