package org.constellation.domain.observation

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.domain.consensus.ConsensusService
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.trust.TrustManager

class ObservationService[F[_]: Concurrent: Logger](trustManager: TrustManager[F], dao: DAO)
    extends ConsensusService[F, Observation] {
  protected[domain] val pending = new PendingObservationsMemPool[F]()

  override def metricRecordPrefix: Option[String] = "Observation".some

  override def accept(o: Observation, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(o)
      .flatTap(_ => trustManager.updateStoredReputation(o))
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("observationAccepted"))
}
