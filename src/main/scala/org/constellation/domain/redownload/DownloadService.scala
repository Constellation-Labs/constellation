package org.constellation.domain.redownload

import cats.effect.{Blocker, Concurrent, ContextShift, LiftIO, Sync}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.checkpoint.{CheckpointAcceptanceService, TopologicalSort}
import org.constellation.genesis.Genesis
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.NodeState
import org.constellation.schema.snapshot.{SnapshotInfo, StoredSnapshot}
import org.constellation.serialization.KryoSerializer
import org.constellation.storage.SnapshotService
import org.constellation.util.Metrics

import scala.concurrent.ExecutionContext

class DownloadService[F[_]](
  redownloadService: RedownloadService[F],
  cluster: Cluster[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics,
  boundedExecutionContext: ExecutionContext,
  unboundedBlocker: Blocker
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

    (cluster.compareAndSet(NodeState.initial, NodeState.ReadyForDownload).flatMap { state =>
      if (state.isNewSet) {
        wrappedDownload.handleErrorWith { error =>
          logger.error(error)(s"Error during download process") >>
            cluster.compareAndSet(NodeState.validDuringDownload, NodeState.PendingDownload).void >>
            error.raiseError[F, Unit]
        }
      } else F.unit
    }) >> redownloadService.acceptCheckpointBlocks().rethrowT
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
            .flatMap { snapshotsSerialized =>
              snapshotsSerialized.traverse { snapshot =>
                C.evalOn(boundedExecutionContext)(F.delay {
                  KryoSerializer.deserializeCast[StoredSnapshot](snapshot)
                })
              }
            }
          snapshotInfoFromMemPool <- redownloadService
            .fetchSnapshotInfo(client)
            .flatMap { snapshotInfo =>
              C.evalOn(boundedExecutionContext)(F.delay {
                KryoSerializer.deserializeCast[SnapshotInfo](snapshotInfo)
              })
            }

          blocksFromSnapshots = acceptedSnapshots.flatMap(_.checkpointCache)

          acceptedBlocksFromSnapshotInfo = snapshotInfoFromMemPool.acceptedCBSinceSnapshotCache
          awaitingBlocksFromSnapshotInfo = snapshotInfoFromMemPool.awaitingCbs

          blocksToAccept = (blocksFromSnapshots ++ acceptedBlocksFromSnapshotInfo ++ awaitingBlocksFromSnapshotInfo).distinct

          _ <- checkpointAcceptanceService.waitingForResolving.modify { blocks =>
            val updated = blocks ++ blocksToAccept.map(_.checkpointBlock.soeHash)
            (updated, ())
          }

          _ <- C
            .evalOn(boundedExecutionContext) {
              F.delay {
                TopologicalSort.sortBlocksTopologically(blocksToAccept).toList
              }
            }
            .flatMap {
              _.traverse { b =>
                logger.debug(s"Accepting block above majority: ${b.height}") >>
                  C.evalOn(boundedExecutionContext) {
                      checkpointAcceptanceService.accept(b)
                    }
                    .handleErrorWith { error =>
                      logger.warn(error)(s"Error during blocks acceptance after download") >> F.unit
                    }
              }
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
        .broadcast(PeerResponse.run(apiClient.checkpoint.getGenesis(), unboundedBlocker))
        .map(_.values.flatMap(_.toOption))
        .map(_.find(_.nonEmpty).flatten.get)
        .flatTap { genesis =>
          ContextShift[F].evalOn(boundedExecutionContext)(F.delay {
            Genesis.acceptGenesis(genesis)
          })
        }
        .void
    } yield ()
}

object DownloadService {

  def apply[F[_]: Concurrent: ContextShift](
    redownloadService: RedownloadService[F],
    cluster: Cluster[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    apiClient: ClientInterpreter[F],
    metrics: Metrics,
    boundedExecutionContext: ExecutionContext,
    unboundedBlocker: Blocker
  ): DownloadService[F] =
    new DownloadService[F](
      redownloadService,
      cluster,
      checkpointAcceptanceService,
      apiClient,
      metrics,
      boundedExecutionContext,
      unboundedBlocker
    )
}
