package org.constellation.storage

import cats.effect.Concurrent
import io.chrisdavenport.log4cats.Logger
import cats.implicits._
import org.constellation.DAO
import org.constellation.primitives.Observation

class ObservationService[F[_]: Concurrent: Logger](dao: DAO) extends ConsensusService[F, Observation] {
  protected[storage] val pending = new PendingObservationsMemPool[F]()

  override def accept(o: Observation): F[Unit] =
    super
      .accept(o)
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("observationAccepted"))
}
