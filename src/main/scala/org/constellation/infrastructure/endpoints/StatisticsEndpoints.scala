package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.implicits._
import io.circe.generic.auto._
import io.circe.syntax._
import org.constellation.domain.transaction.TransactionService
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.{CheckpointCache, Node}
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.RecentDataTracker
import org.constellation.{BlockUIOutput, ChannelValidationInfo}
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class StatisticsEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def endpoints(
    recentBlockTracker: RecentDataTracker[CheckpointCache],
    transactionService: TransactionService[F],
    cluster: Cluster[F]
  ) = dataBlocksEndpoint(recentBlockTracker) <+> dashboardEndpoint(transactionService, cluster)

  private def dataBlocksEndpoint(recentBlockTracker: RecentDataTracker[CheckpointCache]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "data" / "blocks" =>
        F.delay { recentBlockTracker.getAll.toSeq }.map {
          _.map { ccd =>
            val cb = ccd.checkpointBlock

            BlockUIOutput(
              cb.soeHash,
              ccd.height.get.min,
              cb.parentSOEHashes,
              cb.messages.map {
                _.signedMessageData.data.channelId
              }.distinct.map { channelId =>
                ChannelValidationInfo(channelId, true)
              }
            )
          }
        }.map(_.asJson).flatMap(Ok(_))
    }

  private def dashboardEndpoint(
    transactionService: TransactionService[F],
    cluster: Cluster[F]
  ): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "dashboard" =>
        (for {
          txs <- transactionService.getLast20Accepted
          peers <- cluster.clusterNodes().map {
            _.map { node =>
              Node(node.id.address, node.ip.host, node.ip.port)
            }
          }
          response = DashboardResponse(peers, txs).asJson
        } yield response).flatMap(Ok(_))
    }

  case class DashboardResponse(peers: List[Node], transactions: List[TransactionCacheData])
}

object StatisticsEndpoints {

  def ownerEndpoints[F[_]: Concurrent](
    recentBlockTracker: RecentDataTracker[CheckpointCache],
    transactionService: TransactionService[F],
    cluster: Cluster[F]
  ): HttpRoutes[F] =
    new StatisticsEndpoints[F]().endpoints(recentBlockTracker, transactionService, cluster)
}
