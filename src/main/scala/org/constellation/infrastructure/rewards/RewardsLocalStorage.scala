package org.constellation.infrastructure.rewards

import cats.effect.{Concurrent, Sync}
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.storage.LocalFileStorage

class RewardsLocalStorage[F[_]: Concurrent](baseDir: String)(implicit F: Sync[F])
    extends LocalFileStorage[F, StoredRewards](baseDir) {}

object RewardsLocalStorage {

  def apply[F[_]: Concurrent](baseDir: String): RewardsLocalStorage[F] =
    new RewardsLocalStorage[F](baseDir)
}
