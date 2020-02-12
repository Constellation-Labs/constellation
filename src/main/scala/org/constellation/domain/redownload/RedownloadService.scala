package org.constellation.domain.redownload

import cats.effect.{Concurrent, ContextShift}
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.domain.redownload.RedownloadService.{OwnSnapshots, PeersProposals}
import org.constellation.p2p.Cluster
import org.constellation.schema.Id

class RedownloadService[F[_]](cluster: Cluster[F])(implicit F: Concurrent[F], C: ContextShift[F]) {

  // TODO: Consider Height/Hash type classes
  private[redownload] val ownSnapshots: Ref[F, OwnSnapshots] = Ref.unsafe(Map.empty)
  private[redownload] val peersProposals: Ref[F, PeersProposals] = Ref.unsafe(Map.empty)

  def persistOwnSnapshot(height: Long, hash: String): F[Unit] =
    ownSnapshots.modify { m =>
      val updated = if (m.get(height).nonEmpty) m else m.updated(height, hash)
      (updated, ())
    }

  def getOwnSnapshots(): F[OwnSnapshots] = ownSnapshots.get

  def getOwnSnapshot(height: Long): F[Option[String]] =
    ownSnapshots.get.map(_.get(height))

  def fetchPeersProposals(): F[Unit] =
    for {
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.client)
      responses <- apiClients.traverse { client =>
        client
          .getNonBlockingF[F, OwnSnapshots]("/snapshot/own")(C)
          .handleErrorWith(_ => F.pure(Map.empty))
          .map(client.id -> _)
      }
      _ <- peersProposals.modify { m =>
        val updated = m ++ responses.toMap
        (updated, ())
      }
    } yield ()

  // TODO: Check for the alignment ownSnapshots <-> majoritySnapshot and call redownload if needed
  def checkForAlignmentWithMajoritySnapshot(): F[Unit] = ???

}

object RedownloadService {

  def apply[F[_]: Concurrent: ContextShift](cluster: Cluster[F]): RedownloadService[F] =
    new RedownloadService[F](cluster)

  type OwnSnapshots = Map[Long, String] // height -> hash
  type PeersProposals = Map[Id, OwnSnapshots]
}
