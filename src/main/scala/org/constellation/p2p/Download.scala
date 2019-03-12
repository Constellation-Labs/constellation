package org.constellation.p2p

import akka.pattern.ask
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.consensus._
import org.constellation.primitives.PeerManager.Peers
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.APIClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

trait SnapshotsProcessor {
  def processSnapshots(hashes: Seq[String])(implicit peers: Peers): IO[Unit]
}

class RandomSnapshotsProcessor(implicit dao: DAO, ec: ExecutionContext) extends SnapshotsProcessor {
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  val config: Config = ConfigFactory.load()
  private val getSnapshotTimeout = config.getInt("download.getSnapshotTimeout").seconds

  def processSnapshots(hashes: Seq[String])(implicit peers: Peers): IO[Unit] = {
    hashes.map(processSnapshot).toList.parSequence.map(_ => ())
  }

  private def processSnapshot(hash: String)(implicit peers: Peers): IO[Unit] = {
    val clients = peers.values.toArray.map(_.client)

    downloadSnapshot(hash, clients)
      .flatTap { _ =>
        IO {
          dao.metrics.incrementMetric("downloadedSnapshots")
          dao.metrics.incrementMetric("snapshotCount")
        }
      }
      .flatMap(acceptSnapshot)
  }

  private def downloadSnapshot(hash: String, pool: Array[APIClient]): IO[StoredSnapshot] = {
    val stopAt = Random.nextInt(pool.length)

    def makeAttempt(index: Int): IO[StoredSnapshot] =
      getSnapshot(hash, pool(index)).handleErrorWith {
        case e if index == stopAt => IO.raiseError(e)
        case _                    => makeAttempt((index + 1) % pool.length)
      }

    makeAttempt((stopAt + 1) % pool.length)
  }

  private def getSnapshot(hash: String, client: APIClient): IO[StoredSnapshot] = IO.fromFuture {
    IO {
      client.getNonBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + hash,
                                                     timeout = getSnapshotTimeout)
    }
  }

  private def acceptSnapshot(snapshot: StoredSnapshot): IO[Unit] = IO {
    snapshot.checkpointCache.foreach { c =>
      dao.metrics.incrementMetric("downloadedBlocks")
      dao.metrics.incrementMetric("checkpointAccepted")

      c.checkpointBlock.foreach(_.transactions.foreach { _ =>
        dao.metrics.incrementMetric("transactionAccepted")
      })

      better.files
        .File(dao.snapshotPath, snapshot.snapshot.hash)
        .writeByteArray(KryoSerializer.serializeAnyRef(snapshot))
    }
  }
}

class DownloadProcess(snapshotsProcessor: SnapshotsProcessor)(implicit dao: DAO,
                                                              ec: ExecutionContext)
    extends StrictLogging {
  private implicit val ioTimer: Timer[IO] = IO.timer(ec)

  final implicit class FutureOps[+T](f: Future[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(f))
  }

  val config: Config = ConfigFactory.load()
  private val waitForPeersDelay = config.getInt("download.waitForPeers").seconds

  def download(): IO[Unit] =
    for {
      _ <- initDownloadingProcess
      _ <- downloadAndAcceptGenesis
      _ <- waitForPeers()
      peers <- getReadyPeers()
      snapshotClient <- getSnapshotClient(peers)
      majoritySnapshot <- getMajoritySnapshot(peers)
      snapshotHashes <- downloadAndProcessSnapshotsFirstPass(majoritySnapshot)(snapshotClient,
                                                                               peers)
      snapshot <- downloadAndProcessSnapshotsSecondPass(majoritySnapshot, snapshotHashes)(
        snapshotClient,
        peers
      )
      _ <- finishDownload(snapshot)
      _ <- setAcceptedTransactionsAfterDownload
    } yield ()

  private def initDownloadingProcess: IO[Unit] =
    IO(logger.info("Download started"))
      .flatMap(_ => setNodeState(NodeState.DownloadInProgress))
      .flatMap(_ => requestForFaucet)
      .map(_ => ())

  private def downloadAndAcceptGenesis =
    PeerManager
      .broadcast(_.getNonBlocking[Option[GenesisObservation]]("genesis"))
      .map(_.values.flatMap(_.toOption))
      .map(_.find(_.nonEmpty).flatten.get)
      .toIO
      .flatMap(updateMetricAndPass("downloadedGenesis", "true"))
      .flatMap(genesis => IO(dao.acceptGenesis(genesis)).map(_ => genesis))

  private def waitForPeers(): IO[Unit] =
    IO(logger.info(s"Waiting ${waitForPeersDelay.toString()} for peers"))
      .flatMap(_ => IO.sleep(waitForPeersDelay))

  private def getReadyPeers() =
    IO.pure(dao.peerInfo.filter(_._2.peerMetadata.nodeState == NodeState.Ready))

  private def getSnapshotClient(peers: Peers) = IO(peers.head._2.client)

  private def getMajoritySnapshot(peers: Peers): IO[SnapshotInfo] =
    peers.values
      .map(peerData => peerData.client)
      .toList
      .traverse(
        client =>
          client
            .getNonBlockingBytesKryo[SnapshotInfo]("info", timeout = 15.seconds)
            .toIO
      )
      .map(snapshots => snapshots.groupBy(_.snapshot.hash).maxBy(_._2.size)._2.head)

  private def downloadAndProcessSnapshotsFirstPass(snapshotInfo: SnapshotInfo)(
    implicit snapshotClient: APIClient,
    peers: Peers
  ): IO[Seq[String]] =
    for {
      snapshotHashes <- getSnapshotHashes(snapshotInfo)
      _ <- snapshotsProcessor.processSnapshots(snapshotHashes)
      _ <- updateMetric("downloadFirstPassComplete", "true")
      _ <- setNodeState(NodeState.DownloadCompleteAwaitingFinalSync)
    } yield snapshotHashes

  private def downloadAndProcessSnapshotsSecondPass(
    snapshotInfo: SnapshotInfo,
    hashes: Seq[String]
  )(implicit snapshotClient: APIClient, peers: Peers): IO[SnapshotInfo] =
    getSnapshotHashes(snapshotInfo)
      .map(_.filterNot(hashes.contains))
      .flatMap(
        hashes =>
          updateMetric("downloadExpectedNumSnapshotsSecondPass", hashes.size.toString)
            .map(_ => hashes)
      )
      .flatMap(snapshotsProcessor.processSnapshots)
      .flatMap(_ => updateMetric("downloadSecondPassComplete", "true"))
      .map(_ => snapshotInfo)

  private def finishDownload(snapshot: SnapshotInfo): IO[Unit] =
    for {
      _ <- setSnapshot(snapshot)
      _ <- enableRandomTX
      _ <- acceptSnapshotCacheData(snapshot)
      _ <- setNodeState(NodeState.Ready)
      _ <- clearSyncBuffer
      _ <- setDownloadFinishedTime
    } yield ()

  private def setAcceptedTransactionsAfterDownload: IO[Unit] = IO {
    dao.transactionAcceptedAfterDownload =
      dao.metrics.getMetrics.get("transactionAccepted").map(_.toLong).getOrElse(0L)
  }

  /** **/
  private def setNodeState(nodeState: NodeState): IO[Unit] =
    IO(dao.nodeState = nodeState)
      .flatMap(updateMetricAndPass("nodeState", nodeState.toString))
      .flatMap(_ => IO(PeerManager.broadcastNodeState()))

  private def requestForFaucet: IO[Iterable[Response[String]]] =
    Future
      .sequence(
        dao.peerInfo
          .map(_._2.client)
          .map(_.post("faucet", SendToAddress(dao.selfAddressStr, 500L)))
      )
      .toIO

  private def getSnapshotHashes(snapshotInfo: SnapshotInfo): IO[Seq[String]] = {
    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map(_.name)
    val snapshotHashes = snapshotInfo.snapshotHashes.filterNot(preExistingSnapshots.contains)

    IO.pure(snapshotHashes)
      .flatMap(updateMetricAndPass("downloadExpectedNumSnapshots", snapshotHashes.size.toString))
  }

  private def setSnapshot(snapshotInfo: SnapshotInfo): IO[Unit] = IO {
    dao.threadSafeTipService.setSnapshot(snapshotInfo)
  }

  private def enableRandomTX: IO[Unit] = IO {
    dao.generateRandomTX = true
  }

  private def acceptSnapshotCacheData(snapshotInfo: SnapshotInfo): IO[Unit] = IO {
    dao.threadSafeTipService.syncBuffer.foreach { h =>
      if (!snapshotInfo.acceptedCBSinceSnapshotCache.contains(h) && !snapshotInfo.snapshotCache
            .contains(h)) {
        dao.metrics.incrementMetric("syncBufferCBAccepted")
        dao.threadSafeTipService.accept(h)
      }
    }
  }

  private def clearSyncBuffer: IO[Unit] = IO {
    dao.threadSafeTipService.syncBuffer = Seq()
  }

  private def setDownloadFinishedTime: IO[Unit] = IO {
    dao.downloadFinishedTime = System.currentTimeMillis()
  }

  private def updateMetric(key: String, value: String): IO[Unit] = IO {
    dao.metrics.updateMetric(key, value)
  }

  private def updateMetricAndPass[A](key: String, value: String)(a: A): IO[A] =
    IO {
      dao.metrics.updateMetric(key, value)
    }.map(_ => a)
}

object Download {
  def download()(implicit dao: DAO, ec: ExecutionContext): Unit =
    tryWithMetric({
      val snapshotsProcessor = new RandomSnapshotsProcessor()
      val process = new DownloadProcess(snapshotsProcessor)
      process.download().unsafeRunSync()
    }, "download")
}
