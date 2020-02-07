package org.constellation.domain.redownload

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._

class RedownloadService[F[_]]()(implicit F: Concurrent[F]) {

  private[redownload] val ownSnapshots: Ref[F, Map[Long, String]] = Ref.unsafe(Map.empty)

  def persistOwnSnapshot(height: Long, hash: String): F[Unit] =
    ownSnapshots.modify { m =>
      val updated = if (m.get(height).nonEmpty) m else m.updated(height, hash)
      (updated, ())
    }

  def getOwnSnapshots(): F[Map[Long, String]] = ownSnapshots.get

  def getOwnSnapshot(height: Long): F[Option[String]] =
    ownSnapshots.get.map(_.get(height))

}

object RedownloadService {
  def apply[F[_]: Concurrent](): RedownloadService[F] = new RedownloadService[F]()
}
