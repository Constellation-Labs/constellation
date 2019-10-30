package org.constellation.trust

import cats.effect.IO
import cats.implicits._
import com.typesafe.config.Config
import org.constellation.ConfigUtil
import org.constellation.domain.schema.Id
import org.constellation.domain.trust.TrustData
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.PeriodicIO

import scala.concurrent.duration._

class TrustDataPollingScheduler(
  config: Config,
  trustManager: TrustManager[IO],
  cluster: Cluster[IO]
) extends PeriodicIO("TrustDataPollingScheduler") {

  override def trigger(): IO[Unit] =
    cluster.getPeerInfo
      .map(_.filter(t => NodeState.validForLettingOthersDownload.contains(t._2.peerMetadata.nodeState)).values.toList)
      .flatMap(
        _.traverse(
          pd =>
            pd.client
              .getNonBlockingIO[TrustData]("trust")(IO.contextShift(taskPool))
              .handleError(_ => TrustData(pd.client.id, Map.empty[Id, Double]))
        )
      )
      .flatMap(trustManager.handleTrustScoreUpdate)

  schedule(ConfigUtil.getDurationFromConfig("constellation.trust.pull-trust-interval", 60 seconds, config))

}
