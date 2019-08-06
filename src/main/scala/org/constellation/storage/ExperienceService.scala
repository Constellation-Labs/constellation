package org.constellation.storage

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.primitives.Experience

class ExperienceService[F[_]: Concurrent: Logger](dao: DAO) extends ConsensusService[F, Experience] {
  protected[storage] val pending = new PendingExperiencesMemPool[F]()

  override def accept(ex: Experience): F[Unit] =
    super
      .accept(ex)
      .flatTap(_ => Sync[F].delay(dao.metrics.incrementMetric("experienceAccepted")))
}
