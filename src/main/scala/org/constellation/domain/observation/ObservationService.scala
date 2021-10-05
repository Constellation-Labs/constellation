package org.constellation.domain.observation

import cats.effect.Concurrent
import cats.syntax.all._
import org.constellation.DAO
import org.constellation.domain.consensus.{ConsensusService, ConsensusStatus}
import org.constellation.schema.checkpoint.CheckpointCache
import org.constellation.schema.observation.Observation
import org.constellation.schema.transaction.TransactionCacheData
import org.constellation.trust.TrustManager
import org.constellation.util.Metrics

class ObservationService[F[_]: Concurrent](trustManager: TrustManager[F], metrics: Metrics)
    extends ConsensusService[F, Observation] {
  protected[domain] val pending = new PendingObservationsMemPool[F]()

  override def metricRecordPrefix: Option[String] = "Observation".some

  // TODO: updateStoredReputation in `put` but firstly add memoization which observations have been taken into reputation

  override def accept(o: Observation, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(o)
      .flatTap(_ => trustManager.updateObservationReputation(o))
      .flatTap(_ => metrics.incrementMetricAsync[F]("observationAccepted"))

  def applyAfterRedownload(o: Observation, cpc: Option[CheckpointCache]): F[Unit] =
    super
      .accept(o)
      .flatTap(_ => metrics.incrementMetricAsync[F]("observationAccepted"))
      .flatTap(_ => metrics.incrementMetricAsync[F]("observationAcceptedFromRedownload"))

  def setAccepted(blocks: Set[CheckpointCache]): F[Unit] =
    accepted.clear >>
      blocks
        .map(_.checkpointBlock)
        .toList
        .traverse { block =>
          block.observations.toList.traverse { obs =>
            put(obs, ConsensusStatus.Accepted)
          }
        }
        .void
}
