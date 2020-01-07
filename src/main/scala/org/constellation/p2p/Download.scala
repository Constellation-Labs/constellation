package org.constellation.p2p

import cats.effect.{Clock, Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.consensus.EdgeProcessor.{chunkDeSerialize, chunkSerialize}
import org.constellation.consensus.{SnapshotInfo, _}
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.p2p.Cluster.Peers
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Logging.logThread
import org.constellation.util.{APIClient, Distance, FutureTimeTracker, Metrics}
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

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
      }.flatMap(acceptSnapshot(hash, _)),
      "download_processSnapshot"
    ) // TODO: wkoszycki shouldn't we accept sequentially ?
  }

  private def acceptSnapshot(hash: String, rawSnapshot: Array[Byte]): F[Unit] =
    C.evalOn(ConstellationExecutionContext.unbounded)(Sync[F].delay {
      better.files
        .File(dao.snapshotPath, hash)
        .writeByteArray(rawSnapshot)
    })

  private def deserializeStoredSnapshot(storedSnapshotArrayBytes: Array[Byte]) =
    Try(KryoSerializer.deserializeCast[StoredSnapshot](storedSnapshotArrayBytes)).toOption match {
      case Some(value) => value
      case None        => throw new Exception(s"Unable to parse storedSnapshot")
    }
}

class DownloadProcess[F[_]: Concurrent: Timer: Clock](
  snapshotsProcessor: SnapshotsProcessor[F],
  cluster: Cluster[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F]
)(
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

  def updateSnapInfo(majoritySnapshot: SnapshotInfo, snapshotCache: Seq[String]) = {
    val localCps = snapshotCache.toSet
    val acceptedCBSinceSnapshotCacheInfo = majoritySnapshot.acceptedCBSinceSnapshotCache
    val updatedMajority = for {
      localSnapshotCacheData <- LiftIO[F].liftIO(dao.snapshotService.getLocalAcceptedCBSinceSnapshotCache(majoritySnapshot.acceptedCBSinceSnapshot.filter(localCps.contains).toArray))
    } yield majoritySnapshot.copy(acceptedCBSinceSnapshotCache = (acceptedCBSinceSnapshotCacheInfo ++ localSnapshotCacheData))
    updatedMajority
  }


  def reDownload(snapshotHashes: List[String], peers: Map[Id, PeerData]): F[Unit] =
    logThread(
      for {
        snapshotCache <- LiftIO[F].liftIO(dao.snapshotService.getAcceptedCBSinceSnapshot)
//        snapshotCacheData: Seq[CheckpointCache] <- LiftIO[F].liftIO(dao.snapshotService.getLocalAcceptedCBSinceSnapshotCache(snapshotCache.toArray))
        majoritySnapshot <- getMajoritySnapshot(peers, snapshotCache.toArray)//todo need to get acceptedCbsFromSnapshot from snapinfo
        updatedMajoritySnapshot <- updateSnapInfo(majoritySnapshot, snapshotCache)
        _ <- if (snapshotHashes.forall(updatedMajoritySnapshot.snapshotHashes.contains)) Sync[F].unit
        else
          Sync[F].raiseError[Unit](
            new RuntimeException(
              s"[${dao.id.short}] Inconsistent state majority snapshot doesn't contain: ${snapshotHashes
                .filterNot(updatedMajoritySnapshot.snapshotHashes.contains)}"
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
        _ <- finishDownload(updatedMajoritySnapshot)
        _ <- setAcceptedTransactionsAfterDownload()
      } yield (),
      "download_reDownload"
    )

  def testSnapInfoSer(): F[SnapshotInfo] =
    logThread(
      for {
        snapshotCache <- LiftIO[F].liftIO(dao.snapshotService.getAcceptedCBSinceSnapshot)
        peers <- getReadyPeers()
        majoritySnapshot <- getMajoritySnapshot(peers, snapshotCache.toArray)
        updatedMajoritySnapshot <- updateSnapInfo(majoritySnapshot, snapshotCache)
      } yield updatedMajoritySnapshot,
      "testSnapInfoSer"
    )

  def download(): F[Unit] =
    logThread(
      for {
        _ <- initDownloadingProcess
        _ <- downloadAndAcceptGenesis
        _ <- waitForPeers()
        peers <- getReadyPeers()
        snapshotClient <- getSnapshotClient(peers)
        snapshotCache <- LiftIO[F].liftIO(dao.snapshotService.getAcceptedCBSinceSnapshot)
        majoritySnapshot <- getMajoritySnapshot(peers, snapshotCache.toArray)
        updatedMajoritySnapshot <- updateSnapInfo(majoritySnapshot, snapshotCache)
        hashes <- getSnapshotHashes(updatedMajoritySnapshot)
        snapshotHashes <- downloadAndProcessSnapshotsFirstPass(hashes)(
          snapshotClient,
          peers
        )
        missingHashes <- getSnapshotHashes(updatedMajoritySnapshot)
        _ <- downloadAndProcessSnapshotsSecondPass(missingHashes.filterNot(snapshotHashes.contains))(
          snapshotClient,
          peers
        )
        _ <- finishDownload(updatedMajoritySnapshot)
        _ <- setAcceptedTransactionsAfterDownload()
      } yield (),
      "download_download"
    )

  private def initDownloadingProcess: F[Unit] =
    Sync[F]
      .delay(logger.debug("Download started"))
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

//  private[p2p]
  def getMajoritySnapshot(peers: Peers, hashes: Array[String]): F[SnapshotInfo] = {
    val serializedHashes = hashes.grouped(EdgeProcessor.chunkSize).map(t => chunkSerialize(t.toArray, "getMajoritySnapshot")).toArray
    def makeAttempt(clients: List[PeerData]): F[SnapshotInfo] =
      clients match {
        case Nil =>
          Sync[F].raiseError[SnapshotInfo](
            new Exception(
              s"[${dao.id.short}] Unable to get majority snapshot run out of peers, peers size ${peers.size}"
            )
          )
        case head :: tail =>
          head.client.postNonBlockingF[F, Array[Byte]]("snapshot/info",
            serializedHashes,
            45.seconds
          )(C).flatMap{ res =>
                        chainSnapshotInfo(head, res)
                      }.handleErrorWith(e => {
                                  Sync[F]
                                    .delay(logger.error(s"[${dao.id.short}] [Re-Download] Get Majority Snapshot Error : ${e.getMessage}")) >>
                                    makeAttempt(tail)
                                })
      }

    makeAttempt(peers.values.toList)//.map(deserializeSnapshotInfo)
  }

  def chainSnapshotInfo(peer: PeerData, res: Array[Byte]) = {
    logger.error(s"[${dao.id.short}] [Re-Download] chainSnapshotInfo")
    val test = peer.client.getNonBlockingArrayByteF[F]("snapshot/obj/snapshot", timeout = 45.seconds)(C)
    .map{ t: Array[Byte] =>
      logger.error(s"[${dao.id.short}] [Re-Download] chainSnapshotInfo success: ${t}")
      t
    }
    for {
    snh <- test//peer.client.getNonBlockingF[F, Array[Byte]]("snapshot/obj/snapshot", timeout = 12.seconds)(C)
    snapcbs <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/snapshotCBS", timeout = 45.seconds, tag="snapshotCBS")(C)
    sncbs <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/acceptedCBSinceSnapshot", timeout = 45.seconds, tag="acceptedCBSinceSnapshot")(C)
    sncbsc <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/acceptedCBSinceSnapshotCache", timeout = 45.seconds, tag="acceptedCBSinceSnapshotCache")(C)
    lastSnapshotHeight <- peer.client.getNonBlockingArrayByteF[F]("snapshot/obj/lastSnapshotHeight", timeout = 45.seconds)(C)
    snapshotHashes <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/snapshotHashes", timeout = 45.seconds, tag="snapshotHashes")(C)
    addressCacheData <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/addressCacheData", timeout = 45.seconds, tag="addressCacheData")(C)
    tips <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/tips", timeout = 45.seconds, tag="tips")(C)
    snapshotCache <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/snapshotCache", timeout = 45.seconds, tag="snapshotCache")(C)
    lastAcceptedTransactionRef <- peer.client.getNonBlockingFLogged[F, Array[Array[Byte]]]("snapshot/obj/lastAcceptedTransactionRef", timeout = 45.seconds, tag="lastAcceptedTransactionRef")(C)
//    localAcceptedCBSinceSnapshotCache <- snapshotService
    } yield (SnapshotInfo(
      KryoSerializer.deserializeCast[String](snh),
      snapcbs.toSeq.flatMap(chunkDeSerialize[Array[String]](_, "snapshotCBS")),
      sncbs.toSeq.flatMap(chunkDeSerialize[Array[String]](_, "checkpointBlocks")),
      sncbsc.toSeq.flatMap(chunkDeSerialize[Array[CheckpointCache]](_, "acceptedCBSinceSnapshotCache")),
      KryoSerializer.deserializeCast[Int](lastSnapshotHeight),
      snapshotHashes.toSeq.flatMap(chunkDeSerialize[Array[String]](_, "snapshotHashes")),
      addressCacheData.toSeq.flatMap(chunkDeSerialize[Array[(String, AddressCacheData)]](_, "addressCacheData")).toMap,
      tips.toSeq.flatMap(chunkDeSerialize[Array[(String, TipData)]](_, "tips")).toMap,
      snapshotCache.toSeq.flatMap(chunkDeSerialize[Array[CheckpointCache]](_, "snapshotCache")),
      lastAcceptedTransactionRef.toSeq.flatMap(chunkDeSerialize[Array[(String, LastTransactionRef)]](_, "lastAcceptedTransactionRef")).toMap
    ))

  }

  case class SnapshotInfoSer(  snapshot: Array[Byte],
                               checkpointBlocks: Array[Array[Byte]],
                               acceptedCBSinceSnapshot: Array[Array[Byte]],
                               acceptedCBSinceSnapshotCache: Array[Array[Byte]],
                               lastSnapshotHeight: Array[Byte],
                               snapshotHashes: Array[Array[Byte]],
                               addressCacheData: Array[Array[Byte]],
                               tips: Array[Array[Byte]],
                               snapshotCache: Array[Array[Byte]],
                               lastAcceptedTransactionRef: Array[Array[Byte]])

  private def downloadAndProcessSnapshotsFirstPass(snapshotHashes: Seq[String])(
    implicit snapshotClient: APIClient,
    peers: Peers
  ): F[Seq[String]] =
    for {
      _ <- snapshotsProcessor.processSnapshots(snapshotHashes)
      _ <- dao.metrics.updateMetricAsync("downloadFirstPassComplete", "true")
      _ <- cluster.compareAndSet(NodeState.validForDownload, NodeState.DownloadCompleteAwaitingFinalSync)

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
      _ <- storeSnapshotInfo
      _ <- setDownloadFinishedTime()
    } yield ()

  private def setAcceptedTransactionsAfterDownload(): F[Unit] = Sync[F].delay {
    dao.transactionAcceptedAfterDownload = dao.metrics.getMetrics.get("transactionAccepted").map(_.toLong).getOrElse(0L)
    logger.debug("download process has been finished")
  }

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

  private def storeSnapshotInfo: F[Unit] =
    LiftIO[F].liftIO(dao.snapshotService.writeSnapshotInfoToDisk.value.void)

  private def acceptSnapshotCacheData(snapshotInfo: SnapshotInfo): F[Unit] =
    LiftIO[F]
      .liftIO(
        dao.snapshotService
          .syncBufferPull()
          .map(x => filter(x.values.toList, snapshotInfo))
      )
      .flatMap(
        f =>
          f.traverse { h: FinishedCheckpoint =>
            Sync[F].delay(
              logger.debug(
                s"[${dao.id.short}] Sync buffer accept checkpoint block ${h.checkpointCacheData.checkpointBlock.baseHash}"
              )
            ) >> checkpointAcceptanceService.accept(h).recoverWith {
              case _ @(CheckpointAcceptBlockAlreadyStored(_) | TipConflictException(_, _)) =>
                Sync[F].pure(None)
              case unknownError =>
                Sync[F].delay {
                  logger.error(s"[${dao.id.short}] Failed to accept checkpoint block acceptSnapshotCacheData", unknownError)
                } >> Sync[F].pure(None)
            }
          }
      )
      .void

  private def filter(buffer: List[FinishedCheckpoint], info: SnapshotInfo) = {
    val alreadyAccepted =
      (info.acceptedCBSinceSnapshot ++ info.snapshotCache.map(_.checkpointBlock.baseHash)).distinct
    buffer.filterNot(//todo will need to change logic when snapshotCache is filtered
      //todo it's this filter?
      f => alreadyAccepted.contains(f.checkpointCacheData.checkpointBlock.baseHash)
    )
  }

  private def setDownloadFinishedTime(): F[Unit] = Sync[F].delay {
    dao.downloadFinishedTime = System.currentTimeMillis()
  }
}

object Download extends StrictLogging {

  def  getMajoritySnapshotTest()(implicit dao: DAO, ec: ExecutionContext) = {
    tryWithMetric(
      {
        implicit val contextShift = IO.contextShift(ConstellationExecutionContext.bounded)
        implicit val timer = IO.timer(ConstellationExecutionContext.unbounded)
        val snapshotsProcessor =
          new SnapshotsProcessor[IO](SnapshotsDownloader.downloadSnapshotRandomly[IO])
        val process = new DownloadProcess[IO](snapshotsProcessor, dao.cluster, dao.checkpointAcceptanceService)
        process.testSnapInfoSer().map(t => "getMajoritySnapshotTest")//.map(EdgeProcessor.toSnapshotInfoSer(_))
      },
      "getMajoritySnapshotTest"
    )
  }

  // TODO: Remove Try/Future and make it properly chainable
  def download()(implicit dao: DAO, ec: ExecutionContext): Unit =
    if (dao.nodeType == NodeType.Full) {
      tryWithMetric(
        {
          implicit val contextShift = IO.contextShift(ConstellationExecutionContext.bounded)
          implicit val timer = IO.timer(ConstellationExecutionContext.unbounded)
          val snapshotsProcessor =
            new SnapshotsProcessor[IO](SnapshotsDownloader.downloadSnapshotRandomly[IO])
          val process = new DownloadProcess[IO](snapshotsProcessor, dao.cluster, dao.checkpointAcceptanceService)
          val download = process.download()
          val wrappedDownload =
            dao.cluster.compareAndSet(NodeState.validForDownload, NodeState.DownloadInProgress).flatMap {
              case SetStateResult(oldState, true) =>
                download.handleErrorWith { err =>
                  IO.delay(logger.error(s"Download process error: ${err.getMessage}", err))
                    .flatMap(
                      _ =>
                        dao.cluster
                          .compareAndSet(NodeState.validDuringDownload, oldState)
                    )
                    .flatMap(
                      recoverState =>
                        IO.delay(
                          logger.warn(
                            s"Download process error. Trying to set state back to ${oldState}. Result: ${recoverState}"
                          )
                      )
                    )
                }
                download.flatMap(
                  _ =>
                    dao.cluster
                      .compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
                      .flatMap(
                        recoverState =>
                          IO.delay(
                            logger.warn(
                              s"Download process finished. Trying to set state to ${NodeState.Ready}. Result: ${recoverState}"
                            )
                        )
                    )
                )
              case SetStateResult(oldState, _) =>
                IO.delay(
                  logger.warn(s"Download process can't start due to invalid node state: ${oldState}")
                )
            }
          wrappedDownload.unsafeRunAsync(_ => ())
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

      dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready).unsafeRunAsync(_ => ())

    }
}
