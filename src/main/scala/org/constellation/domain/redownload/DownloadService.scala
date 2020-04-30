package org.constellation.domain.redownload

import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.checkpoint.{CheckpointAcceptanceService, TopologicalSort}
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.p2p.Cluster
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.{GenesisObservation, NodeState}
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.SnapshotService
import org.constellation.util.Metrics

class DownloadService[F[_]](
  redownloadService: RedownloadService[F],
  cluster: Cluster[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics
)(
  implicit F: Concurrent[F],
  C: ContextShift[F]
) {

  private val logger = Slf4jLogger.getLogger[F]

  def download()(implicit dao: DAO): F[Unit] = {
    val wrappedDownload = for {
      _ <- clearDataBeforeDownload()
      _ <- downloadAndAcceptGenesis()
      _ <- redownloadService.fetchAndUpdatePeersProposals()
      _ <- redownloadService.checkForAlignmentWithMajoritySnapshot(isDownload = true)
      majorityState <- redownloadService.lastMajorityState.get
      _ <- if (majorityState.isEmpty)
        fetchAndPersistBlocks()
      else F.unit
      _ <- cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
      _ <- metrics.incrementMetricAsync("downloadFinished_total")
    } yield ()

    cluster.compareAndSet(NodeState.initial, NodeState.ReadyForDownload).flatMap { state =>
      if (state.isNewSet) {
        wrappedDownload.handleErrorWith { error =>
          logger.error(s"Error during download process: ${error.getMessage}") >>
            cluster.compareAndSet(NodeState.validDuringDownload, NodeState.PendingDownload).void
        }
      } else F.unit
    }
  }

  private[redownload] def fetchAndPersistBlocks(): F[Unit] =
    for {
      peers <- cluster.getPeerInfo.map(_.values.toList)
      readyPeers = peers.filter(p => NodeState.canActAsDownloadSource(p.peerMetadata.nodeState))
      clients = readyPeers.map(_.peerMetadata.toPeerClientMetadata).toSet
      _ <- redownloadService.useRandomClient(clients) { client =>
        for {
          accepted <- redownloadService.fetchAcceptedSnapshots(client)
          acceptedSnapshots <- accepted.values.toList
            .traverse(hash => redownloadService.fetchSnapshot(hash)(client))
            .map(snapshotsSerialized => snapshotsSerialized.map(KryoSerializer.deserializeCast[StoredSnapshot]))
          snapshotInfoFromMemPool <- redownloadService
            .fetchSnapshotInfo(client)
            .map(KryoSerializer.deserializeCast[SnapshotInfo])

          blocksFromSnapshots = acceptedSnapshots.flatMap(_.checkpointCache)
          acceptedBlocksFromSnapshotInfo = snapshotInfoFromMemPool.acceptedCBSinceSnapshotCache
          awaitingBlocksFromSnapshotInfo = snapshotInfoFromMemPool.awaitingCbs
          blocksToAccept = (blocksFromSnapshots ++ acceptedBlocksFromSnapshotInfo ++ awaitingBlocksFromSnapshotInfo).distinct
          // I need to get blocks back ;/ Not sure if it is efficient enough
          blocksMap = blocksToAccept.map(b => b.checkpointBlock.soeHash -> b).toMap

          blocksToAcceptHashes = blocksToAccept.map(_.checkpointBlock.soeHash)
          sortedHashes = TopologicalSort.sortBlocksTopologically(blocksToAccept)

          _ <- checkpointAcceptanceService.waitingForAcceptance.modify { blocks =>
            val updated = blocks ++ blocksToAccept.map(_.checkpointBlock.soeHash)
            (updated, ())
          }

          toAccept = sortedHashes.filter(blocksMap.contains).map(blocksMap(_))

          _ <- toAccept.traverse { b =>
            logger.debug(s"Accepting block above majority: ${b.height}") >> checkpointAcceptanceService
              .accept(b)
              .handleErrorWith(
                error => logger.warn(s"Error during blocks acceptance after download: ${error.getMessage}") >> F.unit
              )
          }
        } yield ()
      }
    } yield ()

  private[redownload] def clearDataBeforeDownload()(implicit dao: DAO): F[Unit] =
    for {
      _ <- logger.debug("Clearing data before download.")
      _ <- LiftIO[F].liftIO(dao.blacklistedAddresses.clear)
      _ <- LiftIO[F].liftIO(dao.transactionChainService.clear)
      _ <- LiftIO[F].liftIO(dao.addressService.clear)
      _ <- LiftIO[F].liftIO(dao.soeService.clear)
    } yield ()

  private[redownload] def downloadAndAcceptGenesis()(implicit dao: DAO): F[Unit] =
    for {
      _ <- logger.debug("Downloading and accepting genesis.")
      _ <- cluster
        .broadcast(apiClient.checkpoint.getGenesis().run)
        .map(_.values.flatMap(_.toOption))
        .map(_.find(_.nonEmpty).flatten.get)
        .flatTap(genesis => F.delay { Genesis.acceptGenesis(genesis) })
        .void
    } yield ()
}

object DownloadService {

  def apply[F[_]: Concurrent: ContextShift](
    redownloadService: RedownloadService[F],
    cluster: Cluster[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    apiClient: ClientInterpreter[F],
    metrics: Metrics
  ): DownloadService[F] =
    new DownloadService[F](redownloadService, cluster, checkpointAcceptanceService, apiClient, metrics)
}
