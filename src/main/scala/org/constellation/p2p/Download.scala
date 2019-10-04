package org.constellation.p2p

import cats.Parallel
import cats.effect.{Clock, Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus._
import org.constellation.p2p.Cluster.Peers
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.domain.schema.Id
import org.constellation.primitives._
import org.constellation.rollback.CannotLoadSnapshotInfoFile
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{APIClient, Distance, Metrics}
import org.constellation.util.Logging.logThread
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

object SnapshotsDownloader {

  implicit val getSnapshotTimeout: FiniteDuration =
    ConfigUtil.config.getInt("download.getSnapshotTimeout").seconds

  def downloadSnapshotRandomly[F[_]: Concurrent](hash: String, pool: Iterable[APIClient])(
    implicit C: ContextShift[F]
  ): F[Array[Byte]] = {
    val poolArray = pool.toArray
    val stopAt = Random.nextInt(poolArray.length)

    def makeAttempt(index: Int): F[Array[Byte]] =
      getSnapshot(hash, poolArray(index)).handleErrorWith {
        case e if index == stopAt => Sync[F].raiseError[Array[Byte]](e)
        case _                    => makeAttempt((index + 1) % poolArray.length)
      }

    makeAttempt((stopAt + 1) % poolArray.length)
  }

  def downloadSnapshotByDistance[F[_]: Concurrent](hash: String, pool: Iterable[APIClient])(
    implicit C: ContextShift[F]
  ): F[Array[Byte]] = {
    val sortedPeers = pool.toSeq.sortBy(p => Distance.calculate(hash, p.id))

    def makeAttempt(sortedPeers: Iterable[APIClient]): F[Array[Byte]] =
      sortedPeers match {
        case Nil =>
          Sync[F].raiseError[Array[Byte]](new RuntimeException("Unable to download Snapshot from empty peer list"))
        case head :: tail =>
          getSnapshot(hash, head).handleErrorWith {
            case e if tail.isEmpty => Sync[F].raiseError[Array[Byte]](e)
            case _                 => makeAttempt(sortedPeers.tail)
          }
      }

    makeAttempt(sortedPeers)
  }

  private def getSnapshot[F[_]: Concurrent](hash: String, client: APIClient)(
    implicit snapshotTimeout: Duration,
    C: ContextShift[F]
  ): F[Array[Byte]] =
    client
      .getNonBlockingArrayByteF("storedSnapshot/" + hash, timeout = snapshotTimeout)(C)

}

class SnapshotsProcessor[F[_]: Concurrent: Clock](
  downloadSnapshot: (String, Iterable[APIClient]) => F[Array[Byte]]
)(
  implicit dao: DAO,
  ec: ExecutionContext,
  C: ContextShift[F]
) {
  implicit val unsafeLogger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def processSnapshots(hashes: Seq[String])(implicit peers: Peers): F[Unit] =
    hashes.toList.traverse(processSnapshot).void

  private def processSnapshot(hash: String)(implicit peers: Peers): F[Unit] = {
    val clients = peers.values.map(_.client)

    logThread(
      downloadSnapshot(hash, clients).flatTap { _ =>
        dao.metrics.incrementMetricAsync("downloadedSnapshots") >> dao.metrics.incrementMetricAsync(
          Metrics.snapshotCount
        )
      }.flatMap(acceptSnapshot),
      "download_processSnapshot"
    ) // TODO: wkoszycki shouldn't we accept sequentially ?
  }

  private def acceptSnapshot(rawSnapshot: Array[Byte]): F[Unit] = {
    val snapshot = deserializeStoredSnapshot(rawSnapshot)
    logThread(
      snapshot.checkpointCache.toList.traverse { c =>
        dao.metrics.incrementMetricAsync[F]("downloadedBlocks") >>
          dao.metrics.incrementMetricAsync[F](Metrics.checkpointAccepted) >>
          c.checkpointBlock
            .traverse(
              _.transactions.toList.map(_ => dao.metrics.incrementMetricAsync("transactionAccepted")).sequence
            ) >>
          C.evalOn(ConstellationExecutionContext.unbounded)(Sync[F].delay {
            better.files
              .File(dao.snapshotPath, snapshot.snapshot.hash)
              .writeByteArray(rawSnapshot)
          })
      }.void,
      "download_acceptSnapshot"
    )}


  private def deserializeStoredSnapshot(storedSnapshotArrayBytes: Array[Byte]) =
    Try(KryoSerializer.deserializeCast[StoredSnapshot](storedSnapshotArrayBytes)).toOption match {
      case Some(value) => value
      case None        => throw new Exception(s"Unable to parse storedSnapshot")
    }
}

class DownloadProcess[F[_]: Concurrent: Timer: Clock](snapshotsProcessor: SnapshotsProcessor[F], cluster: Cluster[F])(
  implicit dao: DAO,
  ec: ExecutionContext,
  C: ContextShift[F]
) extends StrictLogging {
  implicit val implicitLogger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  final implicit class FutureOps[+T](f: Future[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(f))(IO.contextShift(ec))
  }

  val config: Config = ConfigFactory.load()
  private val waitForPeersDelay = config.getInt("download.waitForPeers").seconds

  def reDownload(snapshotHashes: List[String], peers: Map[Id, PeerData]): F[Unit] =
    logThread(
      for {
        majoritySnapshot <- getMajoritySnapshot(peers)
        _ <- if (snapshotHashes.forall(majoritySnapshot.snapshotHashes.contains)) Sync[F].unit
        else
          Sync[F].raiseError[Unit](
            new RuntimeException(
              s"[${dao.id.short}] Inconsistent state majority snapshot doesn't contain: ${snapshotHashes
                .filterNot(majoritySnapshot.snapshotHashes.contains)}"
            )
          )
        snapshotClient <- getSnapshotClient(peers)
        alreadyDownloaded <- downloadAndProcessSnapshotsFirstPass(snapshotHashes)(
          snapshotClient,
          peers
        )
        _ <- downloadAndProcessSnapshotsSecondPass(snapshotHashes.filterNot(alreadyDownloaded.contains))(
          snapshotClient,
          peers
        )
        _ <- finishDownload(majoritySnapshot)
        _ <- setAcceptedTransactionsAfterDownload()
      } yield (),
      "download_reDownload"
    )

  def download(): F[Unit] =
    logThread(
      for {
        _ <- initDownloadingProcess
        _ <- downloadAndAcceptGenesis
        _ <- waitForPeers()
        peers <- getReadyPeers()
        snapshotClient <- getSnapshotClient(peers)
        majoritySnapshot <- getMajoritySnapshot(peers)
        hashes <- getSnapshotHashes(majoritySnapshot)
        snapshotHashes <- downloadAndProcessSnapshotsFirstPass(hashes)(
          snapshotClient,
          peers
        )
        missingHashes <- getSnapshotHashes(majoritySnapshot)
        _ <- downloadAndProcessSnapshotsSecondPass(missingHashes.filterNot(snapshotHashes.contains))(
          snapshotClient,
          peers
        )
        _ <- finishDownload(majoritySnapshot)
        _ <- setAcceptedTransactionsAfterDownload()
      } yield (),
      "download_download"
    )

  private def initDownloadingProcess: F[Unit] =
    Sync[F]
      .delay(logger.debug("Download started"))
      .flatMap(_ => setNodeState(NodeState.DownloadInProgress))
      .flatMap(_ => requestForFaucet)
      .flatMap(_ => requestForFaucet)
      .flatMap(_ => requestForFaucet)
      .flatMap(_ => Timer[F].sleep(10.seconds))
      .map(_ => ())

  private def downloadAndAcceptGenesis =
    cluster
      .broadcast(_.getNonBlockingF[F, Option[GenesisObservation]]("genesis")(C))
      .map(_.values.flatMap(_.toOption))
      .map(_.find(_.nonEmpty).flatten.get)
      .flatTap(_ => dao.metrics.updateMetricAsync("downloadedGenesis", "true"))
      .flatTap(genesis => Sync[F].delay(Genesis.acceptGenesis(genesis)))

  private def waitForPeers(): F[Unit] =
    Sync[F]
      .delay(logger.debug(s"Waiting ${waitForPeersDelay.toString()} for peers"))
      .flatMap(_ => Timer[F].sleep(waitForPeersDelay)) // mwadon: Should we block the thread by sleep here?

  private def getReadyPeers() =
    LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))

  private def getSnapshotClient(peers: Peers) = peers.head._2.client.pure[F]

  private[p2p] def getMajoritySnapshot(peers: Peers): F[SnapshotInfo] =
    peers.values
      .map(peerData => peerData.client)
      .toList
      .traverse(
        client =>
          client
            .getNonBlockingArrayByteF("info", timeout = 5.seconds)(C)
            .map(snapshotInfoArrayBytes => deserializeSnapshotInfo(snapshotInfoArrayBytes).some)
            .handleErrorWith(e => {
              Sync[F]
                .delay(logger.info(s"[${dao.id.short}] [Re-Download] Get Majority Snapshot Error : ${e.getMessage}")) >>
                Sync[F].pure[Option[SnapshotInfo]](None)
            })
      )
      .map(
        snapshots =>
          if (snapshots.count(_.isEmpty) > (snapshots.size / 2))
            throw new Exception(s"[${dao.id.short}] Unable to get majority snapshot ${snapshots.filter(_.isEmpty)}")
          else snapshots.flatten.groupBy(_.snapshot.hash).maxBy(_._2.size)._2.head
      )

  private def deserializeSnapshotInfo(byteArray: Array[Byte]) =
    Try(KryoSerializer.deserializeCast[SnapshotInfo](byteArray)).toOption match {
      case Some(value) => value
      case None        => throw new Exception(s"[${dao.id.short}] Unable to parse snapshotInfo")
    }

  private def downloadAndProcessSnapshotsFirstPass(snapshotHashes: Seq[String])(
    implicit snapshotClient: APIClient,
    peers: Peers
  ): F[Seq[String]] =
    for {
      _ <- snapshotsProcessor.processSnapshots(snapshotHashes)
      _ <- dao.metrics.updateMetricAsync("downloadFirstPassComplete", "true")
      _ <- setNodeState(NodeState.DownloadCompleteAwaitingFinalSync)
    } yield snapshotHashes

  private def downloadAndProcessSnapshotsSecondPass(
    hashes: Seq[String]
  )(implicit snapshotClient: APIClient, peers: Peers): F[Unit] =
    dao.metrics
      .updateMetricAsync("downloadExpectedNumSnapshotsSecondPass", hashes.size.toString)
      .flatMap(_ => snapshotsProcessor.processSnapshots(hashes))
      .flatTap(_ => dao.metrics.updateMetricAsync("downloadSecondPassComplete", "true"))

  private def finishDownload(snapshot: SnapshotInfo): F[Unit] =
    for {
      _ <- setSnapshot(snapshot)
      _ <- acceptSnapshotCacheData(snapshot)
      _ <- setNodeState(NodeState.Ready)
      _ <- clearSyncBuffer
      _ <- setDownloadFinishedTime()
    } yield ()

  private def setAcceptedTransactionsAfterDownload(): F[Unit] = Sync[F].delay {
    dao.transactionAcceptedAfterDownload = dao.metrics.getMetrics.get("transactionAccepted").map(_.toLong).getOrElse(0L)
    logger.debug("download process has been finished")
  }

  def setNodeState(nodeState: NodeState): F[Unit] =
    cluster.setNodeState(nodeState) >> cluster.broadcastNodeState()

  private def requestForFaucet: F[Iterable[Response[Unit]]] =
    for {
      m <- cluster.getPeerInfo
      clients = m.toList.map(_._2.client)
      resp <- clients.traverse(_.postNonBlockingUnitF("faucet", SendToAddress(dao.selfAddressStr, 500L))(C))
    } yield resp

  private def getSnapshotHashes(snapshotInfo: SnapshotInfo): F[Seq[String]] = {
    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map(_.name)
    val snapshotHashes = snapshotInfo.snapshotHashes.filterNot(preExistingSnapshots.contains)

    snapshotHashes
      .pure[F]
      .flatTap(_ => dao.metrics.updateMetricAsync("downloadExpectedNumSnapshots", snapshotHashes.size.toString))
  }

  private def setSnapshot(snapshotInfo: SnapshotInfo): F[Unit] =
    LiftIO[F].liftIO(dao.snapshotService.setSnapshot(snapshotInfo))

  private def acceptSnapshotCacheData(snapshotInfo: SnapshotInfo): F[Unit] =
    LiftIO[F]
      .liftIO(dao.snapshotService.syncBuffer.get)
      .flatMap(
        _.toList.map { h =>
          if (!snapshotInfo.acceptedCBSinceSnapshotCache.contains(h) && !snapshotInfo.snapshotCache.contains(h)) {
            Sync[F].delay(
              logger.debug(s"[${dao.id.short}] Sync buffer accept checkpoint block ${h.checkpointBlock.get.baseHash}")
            ) >> LiftIO[F].liftIO(dao.checkpointAcceptanceService.accept(h)).recoverWith {
              case _ @(CheckpointAcceptBlockAlreadyStored(_) | TipConflictException(_, _)) =>
                Sync[F].pure(None)
              case unknownError =>
                Sync[F].delay {
                  logger.error(s"[${dao.id.short}] Failed to accept majority checkpoint block", unknownError)
                } >> Sync[F].pure(None)
            }
          } else {
            Sync[F].unit
          }
        }.sequence[F, Unit]
      )
      .void

  private def clearSyncBuffer: F[Unit] =
    LiftIO[F].liftIO(dao.snapshotService.syncBuffer.set(Seq()))

  private def setDownloadFinishedTime(): F[Unit] = Sync[F].delay {
    dao.downloadFinishedTime = System.currentTimeMillis()
  }
}

object Download {

  // TODO: Remove Try/Future and make it properly chainable
  def download()(implicit dao: DAO, ec: ExecutionContext): Unit =
    if (dao.nodeType == NodeType.Full) {
      tryWithMetric(
        {
          implicit val contextShift = IO.contextShift(ConstellationExecutionContext.bounded)
          implicit val timer = IO.timer(ConstellationExecutionContext.unbounded)
          val snapshotsProcessor =
            new SnapshotsProcessor[IO](SnapshotsDownloader.downloadSnapshotRandomly[IO])
          val process = new DownloadProcess[IO](snapshotsProcessor, dao.cluster)
          process.download().unsafeRunAsync(_ => ())
        },
        "download"
      )
    } else {

      // TODO: Move to .lightDownload() from above process, testing separately for now
      // Debug
      val peer = dao.readyPeers(NodeType.Full).unsafeRunSync().head._2.client

      val nearbyChannels = peer.postBlocking[Seq[ChannelMetadata]]("channel/neighborhood", dao.id)

      dao.metrics.updateMetric("downloadedNearbyChannels", nearbyChannels.size.toString)

      nearbyChannels.toList
        .traverse(cmd => dao.channelService.put(cmd.channelId, cmd))
        .unsafeRunSync()

      dao.cluster.setNodeState(NodeState.Ready).unsafeRunSync

    }
}
