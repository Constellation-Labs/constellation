package org.constellation.domain.observation

import cats.effect.Concurrent
import cats.syntax.all._
import org.constellation.DAO
import org.constellation.domain.consensus.ConsensusService
import org.constellation.schema.checkpoint.CheckpointCache
import org.constellation.schema.observation.Observation
import org.constellation.trust.TrustManager

class ObservationService[F[_]: Concurrent](trustManager: TrustManager[F], dao: DAO)
    extends ConsensusService[F, Observation] {
  protected[domain] val pending = new PendingObservationsMemPool[F]()

  override def metricRecordPrefix: Option[String] = "Observation".some

  // TODO: updateStoredReputation in `put` but firstly add memoization which observations have been taken into reputation

  override def accept(o: Observation, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(o)
      .flatTap(_ => trustManager.updateStoredReputation(o))
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("observationAccepted"))

  def applyAfterRedownload(o: Observation, cpc: Option[CheckpointCache]): F[Unit] =
    super
      .accept(o)
      // Why arent we updating trustManager???
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("observationAccepted"))
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("observationAcceptedFromRedownload"))
}
