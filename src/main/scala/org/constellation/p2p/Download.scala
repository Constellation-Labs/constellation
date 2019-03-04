package org.constellation.p2p

import akka.pattern.ask
import cats.effect.{IO, Timer}
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.consensus._
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.APIClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class DownloadProcess(implicit dao: DAO, ec: ExecutionContext) extends StrictLogging {

  type Peers = Map[Schema.Id, PeerData]
  private implicit val ioTimer: Timer[IO] = IO.timer(ec)

  final implicit class FutureOps[+T](f: Future[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(f))
  }

  // TODO: get from config
  private val waitForPeersDelay = 15.seconds

  def download(): IO[Unit] =
    for {
      _ <- initDownloadingProcess
      _ <- downloadAndAcceptGenesis
      _ <- waitForPeers()
      peers <- getReadyPeers()
      snapshotClient <- getSnapshotClient(peers)
      snapshotHashes <- downloadAndProcessSnapshotsFirstPass(snapshotClient, peers)
      snapshot <- downloadAndProcessSnapshotsSecondPass(snapshotHashes)(snapshotClient, peers)
      _ <- finishDownload(snapshot)
      _ <- setAcceptedTransactionsAfterDownload
    } yield ()

  private def initDownloadingProcess: IO[Unit] =
    IO(logger.info("Download started"))
      .flatMap(_ => setNodeState(NodeState.DownloadInProgress))
      .flatMap(_ => requestForFaucet)
      .map(_ => ())

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

  private def downloadAndAcceptGenesis: IO[GenesisObservation] =
    (dao.peerManager ? APIBroadcast(_.getNonBlocking[Option[GenesisObservation]]("genesis")))
      .mapTo[Map[Schema.Id, Future[Option[GenesisObservation]]]]
      .flatMap(m => Future.find(m.values.toList)(_.nonEmpty).map(_.flatten.get))
      .toIO
      .flatMap(updateMetricAndPass("downloadedGenesis", "true"))
      .flatMap(genesis => IO(dao.acceptGenesis(genesis)).map(_ => genesis))

  private def waitForPeers(): IO[Unit] =
    IO(logger.info(s"Waiting ${waitForPeersDelay.toString()} for peers"))
        .flatMap(_ => IO.sleep(waitForPeersDelay))

  private def getReadyPeers(): IO[Peers] =
    (dao.peerManager ? GetPeerInfo)
      .mapTo[Map[Schema.Id, PeerData]]
      .toIO
      .map(_.filter(_._2.peerMetadata.nodeState == NodeState.Ready))

  private def getSnapshotClient(peers: Peers) = IO(peers.head._2.client)

  private def processSnapshots(grouped: Seq[(Seq[String], PeerData)])(implicit peers: Peers) = IO {
    grouped.par.map {
      case (hashes, peer) =>
        hashes.par.map { hash =>
          processSnapshotHash(peers, peer.client, hash)
        }
    }
  }

  // TODO mwadon
  private def processSnapshotHash(peers: Peers, peer: APIClient, hash: String): Boolean = {
    var activePeer = peer
    var remainingPeers: Seq[APIClient] =
      peers.values.map(_.client).filterNot(_ == activePeer).toSeq

    var done = false

    def acceptSnapshot(r: StoredSnapshot) = {
      r.checkpointCache.foreach { c =>
        dao.metrics.incrementMetric("downloadedBlocks")
        // Bypasses tip update / accumulating acceptedSinceCB

        // TODO: Rebuild ledger and verify, turning off for debug
        dao.metrics.incrementMetric("checkpointAccepted")

        //tryWithMetric(acceptCheckpoint(c), "acceptCheckpoint")
        c.checkpointBlock.foreach {
          _.transactions.foreach { tx =>
            //     tx.ledgerApplySnapshot()
            dao.metrics.incrementMetric("transactionAccepted")
            // dao.transactionService.delete(Set(tx.hash))
          }
        }

      }
      //dao.dbActor.putSnapshot(r.snapshot.hash, r.snapshot)
      import better.files._
      File(dao.snapshotPath, r.snapshot.hash).writeByteArray(KryoSerializer.serializeAnyRef(r))
    }

    while (!done && remainingPeers.nonEmpty) {

      val res = Try {
        activePeer
          .getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + hash, timeout = 100.seconds)
      }
      res match {
        case Failure(e) => e.printStackTrace()
        case _          =>
      }
      res.toOption.foreach { r =>
        dao.metrics.incrementMetric("downloadedSnapshots")
        dao.metrics.incrementMetric("snapshotCount")
        acceptSnapshot(r)
      }
      if (res.isFailure) {
        dao.metrics.incrementMetric("downloadSnapshotDataFailed")
      }
      done = res.isSuccess

      if (!done && remainingPeers.nonEmpty) {
        activePeer = remainingPeers.head
        remainingPeers = remainingPeers.filterNot(_ == activePeer)
      }

    }

    done
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

  private def setAcceptedTransactionsAfterDownload: IO[Unit] = IO {
    dao.transactionAcceptedAfterDownload =
      dao.metrics.getMetrics.get("transactionAccepted").map(_.toLong).getOrElse(0L)
  }

  private def downloadAndProcessSnapshotsFirstPass(
    implicit snapshotClient: APIClient,
    peers: Peers
  ): IO[Seq[String]] = {
    val snapshotInfo = downloadSnapshotInfo

    val snapshotHashes = snapshotInfo
      .flatMap(getSnapshotHashes)

    snapshotHashes
      .flatMap(getGroupedHashes)
      .flatMap(processSnapshots)
      .flatMap(_ => updateMetric("downloadFirstPassComplete", "true"))
      .flatMap(_ => setNodeState(NodeState.DownloadCompleteAwaitingFinalSync))
      .flatMap(_ => snapshotHashes)
  }

  private def downloadAndProcessSnapshotsSecondPass(
    hashes: Seq[String]
  )(implicit snapshotClient: APIClient, peers: Peers): IO[SnapshotInfo] = {
    val snapshotInfo = downloadSnapshotInfo

    val snapshotHashes = snapshotInfo
      .flatMap(getSnapshotHashes)
      .map(_.filterNot(hashes.contains))
      .flatMap(
        hashes =>
          updateMetric("downloadExpectedNumSnapshotsSecondPass", hashes.size.toString)
            .map(_ => hashes)
      )

    snapshotHashes
      .flatMap(getGroupedHashes)
      .flatMap(processSnapshots)
      .flatMap(_ => updateMetric("downloadSecondPassComplete", "true"))
      .flatMap(_ => snapshotInfo)
  }

  private def finishDownload(snapshot: SnapshotInfo): IO[Unit] = {
    setSnapshot(snapshot)
      .flatMap(_ => enableRandomTX)
      .flatMap(_ => acceptSnapshotCacheData(snapshot))
      .flatMap(_ => setNodeState(NodeState.Ready))
      .flatMap(_ => clearSyncBuffer)
      .flatMap(_ => setDownloadFinishedTime)
  }

  private def downloadSnapshotInfo(implicit snapshotClient: APIClient): IO[SnapshotInfo] =
    IO { logger.info(s"Downloading from: ${snapshotClient.hostName}:${snapshotClient.apiPort}") }
      .flatMap { _ =>
        snapshotClient
          .getNonBlockingBytesKryo[SnapshotInfo]("info", timeout = 15.seconds)
          .toIO
      }
      .flatMap { a =>
        updateMetric("downloadExpectedNumSnapshotsIncludingPreExisting",
                     a.snapshotHashes.size.toString)
          .map(_ => a)
      }

  private def getSnapshotHashes(snapshotInfo: SnapshotInfo): IO[Seq[String]] = {
    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map(_.name)
    val snapshotHashes = snapshotInfo.snapshotHashes.filterNot(preExistingSnapshots.contains)

    IO.pure(snapshotHashes)
      .flatMap(updateMetricAndPass("downloadExpectedNumSnapshots", snapshotHashes.size.toString))
  }

  private def updateMetricAndPass[A](key: String, value: String)(a: A): IO[A] =
    IO {
      dao.metrics.updateMetric(key, value)
    }.map(_ => a)

  private def getGroupedHashes(
    snapshotHashes: Seq[String]
  )(implicit peers: Peers): IO[Seq[(Seq[String], PeerData)]] = {
    val groupSize = (snapshotHashes.size / peers.size) + 1
    val groupedHashes = snapshotHashes.grouped(groupSize).toSeq
    val grouped = groupedHashes.zip(peers.values)

    updateMetric("downloadGroupSize", groupSize.toString)
      .flatMap(_ => updateMetric("downloadGroupedHashesSize", groupedHashes.size.toString))
      .flatMap(_ => updateMetric("downloadGroupedZipSize", grouped.size.toString))
      .flatMap(_ => updateMetric("downloadGroupCheckSize", grouped.flatMap(_._1).size.toString))
      .map(_ => grouped)
  }

  private def updateMetric(key: String, value: String): IO[Unit] = IO {
    dao.metrics.updateMetric(key, value)
  }
}

object Download extends StrictLogging {

  def download()(implicit dao: DAO, ec: ExecutionContext): Unit =
    tryWithMetric(downloadActual(), "download")

  def downloadActual()(implicit dao: DAO, ec: ExecutionContext): Unit = {
    val process = new DownloadProcess()
    process.download().unsafeRunSync()
  }
}
