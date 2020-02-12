package org.constellation.domain.redownload

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.schema.Id

class RedownloadService[F[_]]()(implicit F: Concurrent[F]) {

  // TODO: Consider Height/Hash type classes
  private[redownload] val ownSnapshots: Ref[F, Map[Long, String]] = Ref.unsafe(Map.empty)
  private[redownload] val peersProposals: Ref[F, Map[Long, (Id, String)]] = Ref.unsafe(Map.empty)

  def persistOwnSnapshot(height: Long, hash: String): F[Unit] =
    ownSnapshots.modify { m =>
      val updated = if (m.get(height).nonEmpty) m else m.updated(height, hash)
      (updated, ())
    }

  def getOwnSnapshots(): F[Map[Long, String]] = ownSnapshots.get

  def getOwnSnapshot(height: Long): F[Option[String]] =
    ownSnapshots.get.map(_.get(height))

  // TODO: Use cluster peers to fetch "/snapshot/own" and merge with peersProposals Map
  def fetchPeersProposals(): F[Unit] = ???

  // TODO: Check for the majority snapshot and store under a variable
  def recalculateMajoritySnapshot(): F[Unit] = ???

  // TODO: Check for the alignment ownSnapshots <-> majoritySnapshot and call redownload if needed
  def checkForAlignmentWithMajoritySnapshot(): F[Unit] = ???

}

object RedownloadService {
  def apply[F[_]: Concurrent](): RedownloadService[F] = new RedownloadService[F]()
}
