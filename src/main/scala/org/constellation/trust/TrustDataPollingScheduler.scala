package org.constellation.trust

import cats.effect.{Blocker, IO}
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.gossip.sampling.PartitionerPeerSampling
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.{Id, NodeState}
import org.constellation.util.{Metrics, PeriodicIO}
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class TrustDataPollingScheduler(
  config: Config,
  trustManager: TrustManager[IO],
  clusterStorage: ClusterStorageAlgebra[IO],
  apiClient: ClientInterpreter[IO],
  partitioners: List[PartitionerPeerSampling[IO]],
  unboundedExecutionContext: ExecutionContext,
  metrics: Metrics
) extends PeriodicIO("TrustDataPollingScheduler", unboundedExecutionContext) {

  override def trigger(): IO[Unit] =
    clusterStorage
      .getActiveFullPeers()
      .map(_.filter(t => NodeState.validForLettingOthersDownload.contains(t._2.peerMetadata.nodeState)).values.toList)
      .flatMap(
        _.traverse(
          pd =>
            PeerResponse
              .run(
                apiClient.cluster
                  .getTrust(),
                Blocker.liftExecutionContext(unboundedExecutionContext)
              )(pd.peerMetadata.toPeerClientMetadata)
              .map(trust => TrustDataInternal(pd.peerMetadata.id, trust.view))
              .handleError(_ => TrustDataInternal(pd.peerMetadata.id, Map.empty[Id, Double]))
        )
      )
      .flatMap { tdi =>
        for {
          selfTdi <- trustManager.getTrustDataInternalSelf
          _ <- trustManager.handleTrustScoreUpdate(tdi)
          _ <- partitioners.traverse(_.repartition(selfTdi, tdi)) //TODO: error handling?
        } yield ()
      }
      .flatTap(_ => metrics.incrementMetricAsync[IO]("trustDataPollingRound"))

  schedule(ConfigUtil.getDurationFromConfig("constellation.trust.pull-trust-interval", 60 seconds, config))

}

object TrustDataPollingScheduler {

  def apply(
    config: Config,
    trustManager: TrustManager[IO],
    clusterStorage: ClusterStorageAlgebra[IO],
    apiClient: ClientInterpreter[IO],
    partitioners: List[PartitionerPeerSampling[IO]],
    unboundedExecutionContext: ExecutionContext,
    metrics: Metrics
  ) =
    new TrustDataPollingScheduler(
      config,
      trustManager,
      clusterStorage,
      apiClient,
      partitioners,
      unboundedExecutionContext,
      metrics
    )
}
