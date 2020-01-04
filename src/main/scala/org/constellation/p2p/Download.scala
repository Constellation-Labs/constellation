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
import org.constellation.consensus.EdgeProcessor.chunkDeSerialize
import org.constellation.consensus.{SnapshotInfo, _}
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.p2p.Cluster.Peers
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Logging.logThread
import org.constellation.util.{APIClient, Distance, Metrics}
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

  def testSnapInfoSer(): F[SnapshotInfo] =
    logThread(
      for {
        peers <- getReadyPeers()
        majoritySnapshot <- getMajoritySnapshot(peers)
      } yield majoritySnapshot,
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
  def getMajoritySnapshot(peers: Peers): F[SnapshotInfo] = {

    def makeAttempt(clients: List[PeerData]): F[SnapshotInfo] =
      clients match {
        case Nil =>
          Sync[F].raiseError[SnapshotInfo](
            new Exception(
              s"[${dao.id.short}] Unable to get majority snapshot run out of peers, peers size ${peers.size}"
            )
          )
        case head :: tail =>
          val t = head.client.getNonBlockingArrayByteF[F]("snapshot/info", timeout = 12.seconds)(C).flatMap{ res =>
                        chainSnapshotInfo(head, res)
                      }.handleErrorWith(e => {
                                  Sync[F]
                                    .delay(logger.error(s"[${dao.id.short}] [Re-Download] Get Majority Snapshot Error : ${e.getMessage}")) >>
                                    makeAttempt(tail)
                                })

//            .flatMap{ _ =>
//            chainSnapshotInfo(head)
//          }.handleErrorWith(e => {
//                      Sync[F]
//                        .delay(logger.error(s"[${dao.id.short}] [Re-Download] Get Majority Snapshot Error : ${e.getMessage}")) >>
//                        makeAttempt(tail)
//                    })
//          head.client
//            .getNonBlockingF[F, SnapshotInfoSer]("snapshot/info", timeout = 12.seconds)(C)
//            .handleErrorWith(e => {
//              Sync[F]
//                .delay(logger.error(s"[${dao.id.short}] [Re-Download] Get Majority Snapshot Error : ${e.getMessage}")) >>
//                makeAttempt(tail)
//            })
          t
      }

    makeAttempt(peers.values.toList)//.map(deserializeSnapshotInfo)
  }

  def chainSnapshotInfo(peer: PeerData, res: Array[Byte]) = {
    logger.error(s"[${dao.id.short}] [Re-Download] chainSnapshotInfo")
    val test = peer.client.getNonBlockingArrayByteF[F]("snapshot/obj/snapshot", timeout = 12.seconds)(C)
    .map{ t: Array[Byte] =>
      logger.error(s"[${dao.id.short}] [Re-Download] chainSnapshotInfo success: ${t}")
      t
    }
    for {
    snh <- test//peer.client.getNonBlockingF[F, Array[Byte]]("snapshot/obj/snapshot", timeout = 12.seconds)(C)
    snapcbs <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/snapshotCBS", timeout = 12.seconds)(C)
    sncbs <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/acceptedCBSinceSnapshot", timeout = 12.seconds)(C)
    sncbsc <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/acceptedCBSinceSnapshotCache", timeout = 12.seconds)(C)
    lastSnapshotHeight <- peer.client.getNonBlockingArrayByteF[F]("snapshot/obj/lastSnapshotHeight", timeout = 12.seconds)(C)
    snapshotHashes <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/snapshotHashes", timeout = 12.seconds)(C)
    addressCacheData <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/addressCacheData", timeout = 12.seconds)(C)
    tips <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/tips", timeout = 12.seconds)(C)
    snapshotCache <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/snapshotCache", timeout = 12.seconds)(C)
    lastAcceptedTransactionRef <- peer.client.getNonBlockingF[F, Array[Array[Byte]]]("snapshot/obj/lastAcceptedTransactionRef", timeout = 12.seconds)(C)
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
//  case class SnapshotInfo(
//                           snapshot: String,
//                           checkpointBlocks: Seq[String] = Seq(),
//                           acceptedCBSinceSnapshot: Seq[String] = Seq(),//todo remove
//                           acceptedCBSinceSnapshotCache: Seq[CheckpointCache] = Seq(),
//                           lastSnapshotHeight: Int = 0,
//                           snapshotHashes: Seq[String] = Seq(),
//                           addressCacheData: Map[String, AddressCacheData] = Map(),
//                           tips: Map[String, TipData] = Map(),
//                           snapshotCache: Seq[CheckpointCache] = Seq(),
//                           lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map()
//                         )

//  private def deserializeSnapshotInfo(snapshotInfoSer: SnapshotInfoSer) =
//    Try(EdgeProcessor.toSnapshotInfo(snapshotInfoSer)) match {
//      case Success(value) => value
//      case Failure(exception) =>
//        throw new Exception(
//          s"[${dao.id.short}] Unable to parse snapshotInfo due to: ${exception.getMessage} with acceptedCBSinceSnapshot.size=${snapshotInfoSer.lastSnapshotHeight}",
//          exception
//        )
//    }


//  private def deserializeSnapshotInfo(byteArray: Array[Byte]) =
//    Try(KryoSerializer.deserializeCast[SnapshotInfo](byteArray)) match {
//      case Success(value) => value
//      case Failure(exception) =>
//        throw new Exception(
//          s"[${dao.id.short}] Unable to parse snapshotInfo due to: ${exception.getMessage} with byteArray.size=${byteArray.size}",
//          exception
//        )
//    }


//  private def deserializeSnapshotInfo(byteArray: Array[Byte]) =
//    Try{
//      val snapInfoSer = KryoSerializer.deserializeCast[SnapshotInfoSer](byteArray)
//      Sync[F].delay(logger.warn(s"deserializeSnapshotInfoRef Success for snapshot height: ${snapInfoSer.lastSnapshotHeight}"))
//      val snapInfo = EdgeProcessor.toSnapshotInfo(snapInfoSer)
//      Sync[F].delay(logger.warn(s"deserializeSnapshotInfo Success for snapshot hash: ${snapInfo.snapshot.hash}"))
//      snapInfo
//    } match {
//      case Success(value: SnapshotInfo) =>
//        Sync[F].delay(logger.warn(s"deserializeSnapshotInfo Success for snapshot height: ${value.snapshot.hash}"))
//        value
//      case Failure(exception) =>
//        throw new Exception(
//          s"[${dao.id.short}] Unable to parse snapshotInfo due to: ${exception.getMessage} with byteArray.size=${byteArray.size}",
//          exception
//        )
//    }

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
          f.traverse { h =>
            Sync[F].delay(
              logger.debug(
                s"[${dao.id.short}] Sync buffer accept checkpoint block ${h.checkpointCacheData.checkpointBlock.baseHash}"
              )
            ) >> checkpointAcceptanceService.accept(h).recoverWith {
              case _ @(CheckpointAcceptBlockAlreadyStored(_) | TipConflictException(_, _)) =>
                Sync[F].pure(None)
              case unknownError =>
                Sync[F].delay {
                  logger.error(s"[${dao.id.short}] Failed to accept majority checkpoint block", unknownError)
                } >> Sync[F].pure(None)
            }
          }
      )
      .void

  private def filter(buffer: List[FinishedCheckpoint], info: SnapshotInfo) = {
    val alreadyAccepted =
      (info.acceptedCBSinceSnapshot ++ info.snapshotCache.map(_.checkpointBlock.baseHash)).distinct
    buffer.filterNot(
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
