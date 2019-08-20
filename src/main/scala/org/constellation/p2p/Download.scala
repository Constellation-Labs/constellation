package org.constellation.p2p

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.consensus._
import org.constellation.p2p.Cluster.Peers
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{APIClient, Distance, Metrics}
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object SnapshotsDownloader {

  implicit val getSnapshotTimeout: FiniteDuration =
    ConfigUtil.config.getInt("download.getSnapshotTimeout").seconds

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def downloadSnapshotRandomly(hash: String, pool: Iterable[APIClient]): IO[StoredSnapshot] = {
    val poolArray = pool.toArray
    val stopAt = Random.nextInt(poolArray.length)

    def makeAttempt(index: Int): IO[StoredSnapshot] =
      getSnapshot(hash, poolArray(index)).handleErrorWith {
        case e if index == stopAt => IO.raiseError[StoredSnapshot](e)
        case _                    => makeAttempt((index + 1) % poolArray.length)
      }

    makeAttempt((stopAt + 1) % poolArray.length)
  }

  def downloadSnapshotByDistance(hash: String, pool: Iterable[APIClient]): IO[StoredSnapshot] = {
    val sortedPeers = pool.toSeq.sortBy(p => Distance.calculate(hash, p.id))

    def makeAttempt(sortedPeers: Iterable[APIClient]): IO[StoredSnapshot] =
      sortedPeers match {
        case Nil =>
          IO.raiseError[StoredSnapshot](new RuntimeException("Unable to download Snapshot from empty peer list"))
        case head :: tail =>
          getSnapshot(hash, head).handleErrorWith {
            case e if tail.isEmpty => IO.raiseError[StoredSnapshot](e)
            case _                 => makeAttempt(sortedPeers.tail)
          }
      }

    makeAttempt(sortedPeers)
  }

  private def getSnapshot(hash: String, client: APIClient)(
    implicit snapshotTimeout: Duration
  ): IO[StoredSnapshot] = IO.fromFuture {
    IO {
      client.getNonBlockingBytesKryo[StoredSnapshot](
        "storedSnapshot/" + hash,
        timeout = snapshotTimeout
      )
    }
  }
}

class SnapshotsProcessor(downloadSnapshot: (String, Iterable[APIClient]) => IO[StoredSnapshot])(
  implicit dao: DAO,
  ec: ExecutionContext
) {
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  def processSnapshots(hashes: Seq[String])(implicit peers: Peers): IO[Unit] =
    hashes.map(processSnapshot).toList.parSequence.map(_ => ())

  private def processSnapshot(hash: String)(implicit peers: Peers): IO[Unit] = {
    val clients = peers.values.map(_.client)

    downloadSnapshot(hash, clients).flatTap { _ =>
      IO {
        dao.metrics.incrementMetric("downloadedSnapshots")
        dao.metrics.incrementMetric(Metrics.snapshotCount)
      }
    }.flatMap(acceptSnapshot) // TODO: wkoszycki shouldn't we accept sequentially ?
  }

  private def acceptSnapshot(snapshot: StoredSnapshot): IO[Unit] = IO {
    snapshot.checkpointCache.foreach { c =>
      dao.metrics.incrementMetric("downloadedBlocks")
      dao.metrics.incrementMetric(Metrics.checkpointAccepted)

      c.checkpointBlock.foreach(_.transactions.foreach { _ =>
        dao.metrics.incrementMetric("transactionAccepted")
      })

      better.files
        .File(dao.snapshotPath, snapshot.snapshot.hash)
        .writeByteArray(KryoSerializer.serializeAnyRef(snapshot))
    }
  }
}

class DownloadProcess(snapshotsProcessor: SnapshotsProcessor)(implicit dao: DAO, ec: ExecutionContext)
    extends StrictLogging {
  private implicit val ioTimer: Timer[IO] = IO.timer(ec)

  final implicit class FutureOps[+T](f: Future[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(f))(IO.contextShift(ec))
  }

  val config: Config = ConfigFactory.load()
  private val waitForPeersDelay = config.getInt("download.waitForPeers").seconds

  def reDownload(snapshotHashes: List[String], peers: Map[Id, PeerData]): IO[Unit] =
    for {
      majoritySnapshot <- getMajoritySnapshot(peers)
      _ <- if (snapshotHashes.forall(majoritySnapshot.snapshotHashes.contains)) IO.unit
      else
        IO.raiseError[Unit](
          new RuntimeException(
            s"Inconsistent state majority snapshot doesn't contain: ${snapshotHashes.filterNot(majoritySnapshot.snapshotHashes.contains)}"
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
    } yield ()

  def download(): IO[Unit] =
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
    } yield ()

  private def initDownloadingProcess: IO[Unit] =
    IO(logger.debug("Download started"))
      .flatMap(_ => setNodeState(NodeState.DownloadInProgress))
      .flatMap(_ => requestForFaucet)
      .flatMap(_ => requestForFaucet)
      .flatMap(_ => requestForFaucet)
      .flatMap(_ => IO.sleep(10.seconds))
      .map(_ => ())

  private def downloadAndAcceptGenesis =
    dao.cluster
      .broadcast(_.getNonBlockingIO[Option[GenesisObservation]]("genesis"))
      .map(_.values.flatMap(_.toOption))
      .map(_.find(_.nonEmpty).flatten.get)
      .flatTap(_ => dao.metrics.updateMetricAsync[IO]("downloadedGenesis", "true"))
      .flatTap(genesis => IO(Genesis.acceptGenesis(genesis)))

  private def waitForPeers(): IO[Unit] =
    IO(logger.debug(s"Waiting ${waitForPeersDelay.toString()} for peers"))
      .flatMap(_ => IO.sleep(waitForPeersDelay))

  private def getReadyPeers() =
    dao.readyPeers(NodeType.Full)

  private def getSnapshotClient(peers: Peers) = IO(peers.head._2.client)

  private[p2p] def getMajoritySnapshot(peers: Peers): IO[SnapshotInfo] =
    peers.values
      .map(peerData => peerData.client)
      .toList
      .traverse(
        client =>
          client
            .getNonBlockingBytesKryoTry[SnapshotInfo]("info", timeout = 15.seconds)
            .toIO
      )
      .map(
        snapshots =>
          if (snapshots.count(_.isFailure) > (snapshots.size / 2))
            throw new Exception(s"Unable to get majority snapshot ${snapshots.filter(_.isFailure)}")
          else snapshots.flatMap(_.toOption).groupBy(_.snapshot.hash).maxBy(_._2.size)._2.head
      )

  private def downloadAndProcessSnapshotsFirstPass(snapshotHashes: Seq[String])(
    implicit snapshotClient: APIClient,
    peers: Peers
  ): IO[Seq[String]] =
    for {
      _ <- snapshotsProcessor.processSnapshots(snapshotHashes)
      _ <- dao.metrics.updateMetricAsync[IO]("downloadFirstPassComplete", "true")
      _ <- setNodeState(NodeState.DownloadCompleteAwaitingFinalSync)
    } yield snapshotHashes

  private def downloadAndProcessSnapshotsSecondPass(
    hashes: Seq[String]
  )(implicit snapshotClient: APIClient, peers: Peers): IO[Unit] =
    dao.metrics
      .updateMetricAsync[IO]("downloadExpectedNumSnapshotsSecondPass", hashes.size.toString)
      .flatMap(_ => snapshotsProcessor.processSnapshots(hashes))
      .flatTap(_ => dao.metrics.updateMetricAsync[IO]("downloadSecondPassComplete", "true"))

  private def finishDownload(snapshot: SnapshotInfo): IO[Unit] =
    for {
      _ <- setSnapshot(snapshot)
      _ <- acceptSnapshotCacheData(snapshot)
      _ <- setNodeState(NodeState.Ready)
      _ <- clearSyncBuffer
      _ <- setDownloadFinishedTime()
    } yield ()

  private def setAcceptedTransactionsAfterDownload(): IO[Unit] = IO {
    dao.transactionAcceptedAfterDownload = dao.metrics.getMetrics.get("transactionAccepted").map(_.toLong).getOrElse(0L)
    logger.debug("download process has been finished")
  }

  def setNodeState(nodeState: NodeState): IO[Unit] =
    dao.cluster.setNodeState(nodeState) *> dao.cluster.broadcastNodeState()

  private def requestForFaucet: IO[Iterable[Response[String]]] =
    for {
      m <- dao.peerInfo
      clients = m.toList.map(_._2.client)
      resp <- clients.traverse(_.post("faucet", SendToAddress(dao.selfAddressStr, 500L)).toIO)
    } yield resp

  private def getSnapshotHashes(snapshotInfo: SnapshotInfo): IO[Seq[String]] = {
    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map(_.name)
    val snapshotHashes = snapshotInfo.snapshotHashes.filterNot(preExistingSnapshots.contains)

    IO.pure(snapshotHashes)
      .flatTap(_ => dao.metrics.updateMetricAsync[IO]("downloadExpectedNumSnapshots", snapshotHashes.size.toString))
  }

  private def setSnapshot(snapshotInfo: SnapshotInfo): IO[Unit] =
    dao.snapshotService.setSnapshot(snapshotInfo)

  private def acceptSnapshotCacheData(snapshotInfo: SnapshotInfo): IO[Unit] =
    dao.snapshotService.syncBuffer.get
      .flatMap(_.toList.map { h =>
        if (!snapshotInfo.acceptedCBSinceSnapshotCache.contains(h) && !snapshotInfo.snapshotCache.contains(h)) {
          dao.metrics.incrementMetricAsync[IO]("SyncBufferCBAccepted") *> dao.checkpointService.accept(h).recoverWith {
            case _ @(CheckpointAcceptBlockAlreadyStored(_) | TipConflictException(_, _)) =>
              IO.pure(None)
            case unknownError =>
              IO {
                logger.error(s"[${dao.id.short}] Failed to accept majority checkpoint block", unknownError)
              } >> IO.pure(None)
          }
        } else {
          IO.unit
        }
      }.sequence[IO, Unit])
      .void

  private def clearSyncBuffer: IO[Unit] =
    dao.snapshotService.syncBuffer.set(Seq())

  private def setDownloadFinishedTime(): IO[Unit] = IO {
    dao.downloadFinishedTime = System.currentTimeMillis()
  }
}

object Download {

  def download()(implicit dao: DAO, ec: ExecutionContext): Unit =
    if (dao.nodeType == NodeType.Full) {
      tryWithMetric(
        {
          val snapshotsProcessor =
            new SnapshotsProcessor(SnapshotsDownloader.downloadSnapshotRandomly)
          val process = new DownloadProcess(snapshotsProcessor)
          process.download().unsafeRunSync()
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
