package org.constellation.domain.observation

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.domain.consensus.ConsensusService
import org.constellation.primitives.Schema.CheckpointCache

class ObservationService[F[_]: Concurrent: Logger](dao: DAO) extends ConsensusService[F, Observation] {
  protected[domain] val pending = new PendingObservationsMemPool[F]()

  override def accept(o: Observation, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(o)
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("observationAccepted"))
}
