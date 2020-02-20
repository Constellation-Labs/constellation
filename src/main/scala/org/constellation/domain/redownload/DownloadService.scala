package org.constellation.domain.redownload

import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.p2p.Cluster
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.{GenesisObservation, NodeState}

class DownloadService[F[_]](redownloadService: RedownloadService[F], cluster: Cluster[F])(
  implicit F: Concurrent[F],
  C: ContextShift[F]
) {

  private val logger = Slf4jLogger.getLogger[F]

  def download()(implicit dao: DAO): F[Unit] = {
    val wrappedDownload = for {
      _ <- clearDataBeforeDownload()
      _ <- requestForFaucet()
      _ <- downloadAndAcceptGenesis()
      _ <- redownloadService.fetchAndUpdatePeersProposals
      _ <- redownloadService.checkForAlignmentWithMajoritySnapshot()
      _ <- cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
    } yield ()

    cluster.compareAndSet(NodeState.initial, NodeState.ReadyForDownload).flatMap { state =>
      if (state.isNewSet) {
        wrappedDownload.handleErrorWith { _ =>
          cluster.compareAndSet(NodeState.validDuringDownload, NodeState.PendingDownload).void
        }
      } else F.unit
    }

  }

  // TODO: to be implemented
  private[redownload] def requestForFaucet(): F[Unit] = F.unit

  private[redownload] def clearDataBeforeDownload()(implicit dao: DAO): F[Unit] =
    for {
      _ <- logger.debug("Clearing data before download.")
      _ <- LiftIO[F].liftIO(dao.blacklistedAddresses.clear)
      _ <- LiftIO[F].liftIO(dao.transactionChainService.clear)
      _ <- LiftIO[F].liftIO(dao.addressService.clear)
      _ <- LiftIO[F].liftIO(dao.soeService.clear)
    } yield ()

  private[redownload] def downloadAndAcceptGenesis()(implicit dao: DAO): F[Unit] = for {
    _ <- logger.debug("Downloading and accepting genesis.")
    _ <- cluster
      .broadcast(_.getNonBlockingF[F, Option[GenesisObservation]]("genesis")(C))
      .map(_.values.flatMap(_.toOption))
      .map(_.find(_.nonEmpty).flatten.get)
      .flatTap(genesis => F.delay { Genesis.acceptGenesis(genesis) })
      .void
  } yield ()
}

object DownloadService {

  def apply[F[_]: Concurrent: ContextShift](
    redownloadService: RedownloadService[F],
    cluster: Cluster[F]
  ): DownloadService[F] =
    new DownloadService[F](redownloadService, cluster)
}
