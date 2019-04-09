package org.constellation.consensus

import java.io.IOException
import java.nio.file.Path

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Validation.EnrichedFuture
import org.constellation.util.{APIClient, Distance, HashSignature, Signable}
import org.constellation.{ConfigUtil, DAO}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

case class CreateCheckpointEdgeResponse(
  checkpointEdge: CheckpointEdge,
  transactionsUsed: Set[String],
  // filteredValidationTips: Seq[SignedObservationEdge],
  updatedTransactionMemPoolThresholdMet: Set[String]
)

case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

case class SignatureResponse(signature: Option[HashSignature], reRegister: Boolean = false)
case class FinishedCheckpoint(checkpointCacheData: CheckpointCacheData, facilitators: Set[Id])

case class FinishedCheckpointResponse(reRegister: Boolean = false)

object EdgeProcessor extends StrictLogging {

  def acceptCheckpoint(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = {

    if (checkpointCacheData.checkpointBlock.isEmpty) {
      dao.metrics.incrementMetric("acceptCheckpointCalledWithEmptyCB")
    } else {

      val cb = checkpointCacheData.checkpointBlock.get

      cb.storeSOE()

      val height = cb.calculateHeight()

      val fallbackHeight =
        if (height.isEmpty) checkpointCacheData.height else height

      if (fallbackHeight.isEmpty) {
        dao.metrics.incrementMetric("heightEmpty")
      } else {
        dao.metrics.incrementMetric("heightNonEmpty")
      }

      dao.checkpointService.memPool.putSync(cb.baseHash, checkpointCacheData)
      // dao.checkpointService.midDb.put(cb.baseHash, checkpointCacheData).unsafeRunSync()

      cb.messages.foreach { m =>
        if (m.signedMessageData.data.previousMessageHash != Genesis.CoinBaseHash) {
          dao.messageService.putSync(
            m.signedMessageData.data.channelId,
            ChannelMessageMetadata(m, Some(cb.baseHash))
          )
          dao.channelService.updateOnly(
            m.signedMessageData.hash, { cmd =>
              val slicedMessages = cmd.last25MessageHashes.slice(0, 25)
              cmd.copy(
                totalNumMessages = cmd.totalNumMessages + 1,
                last25MessageHashes = Seq(m.signedMessageData.hash) ++ slicedMessages
              )
            }
          )
        } else { // Unsafe json extract
          dao.channelService.putSync(
            m.signedMessageData.hash,
            ChannelMetadata(
              m.signedMessageData.data.message.x[ChannelOpen],
              ChannelMessageMetadata(m, Some(cb.baseHash))
            )
          )
        }
        dao.messageService
          .putSync(m.signedMessageData.hash, ChannelMessageMetadata(m, Some(cb.baseHash)))
        dao.metrics.incrementMetric("messageAccepted")
      }

      // Accept transactions
      acceptTransactions(cb).unsafeRunSync()

      dao.metrics.incrementMetric("checkpointAccepted")
      val data = CheckpointCacheData(
        Some(cb),
        height = fallbackHeight
      )
      cb.store(
        data
      )

    }
  }

  private def acceptTransactions(cb: CheckpointBlock)(implicit dao: DAO): IO[Unit] = {
    def toCacheData(tx: Transaction) = TransactionCacheData(
      tx,
      valid = true,
      inMemPool = false,
      inDAG = true,
      Map(cb.baseHash -> true),
      resolved = true,
      cbBaseHash = Some(cb.baseHash)
    )

    cb.transactions.toList
      .map(tx ⇒ (tx, toCacheData(tx)))
      .map {
        case (tx, txMetadata) ⇒
          IO(dao.transactionService.memPool)
        //    .remove(tx.baseHash)
        //    .flatMap(_ ⇒ dao.transactionService.midDb.put(tx.baseHash, txMetadata))
            .flatMap(_ ⇒ dao.metrics.incrementMetricAsync("transactionAccepted"))
            .flatMap(_ ⇒ dao.addressService.transfer(tx))
      }
      .sequence[IO, AddressCacheData]
      .map(_ ⇒ ())
  }

  private def requestBlockSignature(
    checkpointBlock: CheckpointBlock,
    finalFacilitators: Set[
      Id
    ],
    data: PeerData
  )(implicit dao: DAO, ec: ExecutionContext) = {
    async {
      val sigResp = await(
        data.client.postNonBlocking[SignatureResponse](
          "request/signature",
          SignatureRequest(checkpointBlock, finalFacilitators + dao.id),
          15.seconds
        )
      )

      if (sigResp.reRegister) {
        // PeerManager.attemptRegisterPeer() TODO : Finish
      }

      sigResp.signature
    }
  }

  private def processSignedBlock(
    cache: CheckpointCacheData,
    finalFacilitators: Set[
      Id
    ]
  )(implicit dao: DAO, ec: ExecutionContext) = {

    val responses = dao.peerInfo.values.toList.map { peer =>
      wrapFutureWithMetric(
        peer.client.postNonBlocking[Option[FinishedCheckpointResponse]](
          "finished/checkpoint",
          FinishedCheckpoint(cache, finalFacilitators),
          timeout = 20.seconds
        ),
        "finishedCheckpointBroadcast"
      )
    }

    responses.traverse(_.toValidatedNel).map(_.sequence)
  }

  def formCheckpoint(messages: Seq[ChannelMessage] = Seq())(
    implicit dao: DAO
  ) = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    val transactions =
      dao.threadSafeTXMemPool.pullUpTo(dao.maxTXInBlock)

    dao.metrics.incrementMetric("attemptFormCheckpointCalls")

    if (transactions.isEmpty) {
      dao.metrics.incrementMetric("attemptFormCheckpointNoTX")
    }
    val readyFacilitators = dao.readyFacilitators()

    if (readyFacilitators.isEmpty) {
      dao.metrics.incrementMetric("attemptFormCheckpointInsufficientTipsOrFacilitators")
      if (dao.nodeConfig.isGenesisNode) {
        val maybeTips = dao.concurrentTipService.pull(Map.empty)(dao.metrics)
        if (maybeTips.isEmpty) {
          dao.metrics.incrementMetric("attemptFormCheckpointNoGenesisTips")
        }
        maybeTips.foreach {
          case (tipSOE, _) =>
            val checkpointBlock =
              CheckpointBlock.createCheckpointBlock(transactions, tipSOE.map { soe =>
                TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash, Some(soe.baseHash))
              }, messages)(dao.keyPair)

            val cache =
              CheckpointCacheData(
                Some(checkpointBlock),
                height = checkpointBlock.calculateHeight()
              )

            dao.threadSafeSnapshotService.accept(cache)
            dao.threadSafeMessageMemPool.release(messages)

        }
      }

    }

    val result = dao.concurrentTipService.pull(readyFacilitators)(dao.metrics).map {
      case (tipSOE, facils) =>
        // Change to method on TipsReturned // abstract for reuse.
        val checkpointBlock = CheckpointBlock.createCheckpointBlock(transactions, tipSOE.map {
          soe =>
            TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash, Some(soe.baseHash))
        }, messages)(dao.keyPair)
        dao.metrics.incrementMetric("checkpointBlocksCreated")

        val finalFacilitators = facils.keySet

        val signatureResults = facils.values.toList
          .traverse { peerData =>
            requestBlockSignature(checkpointBlock, finalFacilitators, peerData).toValidatedNel
          }
          .flatMap { signatureResultList =>
            signatureResultList.sequence
              .map { signatures =>
                // Unsafe flatten -- revisit during consensus updates
                signatures.flatten.foldLeft(checkpointBlock) {
                  case (cb, hs) =>
                    cb.plus(hs)
                }
              }
              .ensure(NonEmptyList.one(new Throwable("Invalid CheckpointBlock")))(
                _.simpleValidation()
              )
              .traverse { finalCB =>
                val cache = CheckpointCacheData(finalCB.some, height = finalCB.calculateHeight())
                dao.threadSafeSnapshotService.accept(cache)
                processSignedBlock(
                  cache,
                  finalFacilitators
                )
              }
          }

        wrapFutureWithMetric(signatureResults, "checkpointBlockFormation")

        signatureResults.foreach {
          case Valid(_) =>
          case Invalid(failures) =>
            failures.toList.foreach { e =>
              dao.metrics.incrementMetric(
                "formCheckpointSignatureResponseError"
              )
              logger.warn("Failure gathering signature", e)
            }

        }

        // using transform kind of like a finally for Future.
        // I want to ensure the locks get cleaned up
        signatureResults.transform { res =>
          // Cleanup locks

          dao.threadSafeMessageMemPool.release(messages)
          res.map(_ => true)
        }
    }
    result.sequence
  }

  def resolveTransactions(
    hashes: Seq[String],
    priorityPeer: Id
  )(implicit dao: DAO): Future[Seq[TransactionCacheData]] = {

    implicit val exec: ExecutionContextExecutor = dao.signatureExecutionContext

    def lookupTransaction(hash: String, client: APIClient): Future[Option[TransactionCacheData]] =
      client.getNonBlocking[Option[TransactionCacheData]](
        s"transaction/$hash",
        timeout = 4.seconds
      )

    def resolveTransaction(hash: String,
                           peers: Seq[APIClient]): Future[Option[TransactionCacheData]] = {

      var remainingPeers = peers.tail

      val lookupResult = lookupTransaction(hash, peers.head)
      lookupResult.recoverWith {
        // TODO: handle more exceptions here by collecting them all
        case _ => Future.failed(new Exception("Ran out of peers to query for transaction"))
      }
    }

    // Change to facilitator ordering

    val (priority, nonPriority) = dao.readyPeers.toSeq.partition(_._1 == priorityPeer)
    val peerData = priority ++ nonPriority
    val peers = peerData.map { _._2.client }
    val results = hashes.map { hash =>
      resolveTransaction(hash, peers)
    }
    val seq = Future.sequence(results)
    seq.transformWith {
      case Success(responses) if responses.forall(_.nonEmpty) =>
        Future.successful(responses.flatten)
      case _ => Future.failed(new Exception("Failed to resolve transactions"))
    }

  }

  def resolveChannelMessages(
    hashes: Seq[String],
    priorityPeer: Id
  )(implicit dao: DAO): Future[Seq[ChannelMessageMetadata]] = {
    implicit val exec: ExecutionContextExecutor = dao.signatureExecutionContext

    def lookupChannelMessage(hash: String, client: APIClient): Future[Option[ChannelMessageMetadata]] =
      client.getNonBlocking[Option[ChannelMessageMetadata]](
        s"message/$hash",
        timeout = 4.seconds
      )

    def resolveChannelMessage(
      hash: String,
      peers: Seq[APIClient]
    ): Future[Option[ChannelMessageMetadata]] = {
      val remainingPeers = peers.tail
      val lookupResult = lookupChannelMessage(hash, peers.head)
      lookupResult.recoverWith {
        case _ ⇒ Future.failed(new Exception("Ran out of peers to query for channel-message"))
      }
    }

    val (priority, nonPriority) = dao.readyPeers.toSeq.partition(_._1 == priorityPeer)
    val peerData = priority ++ nonPriority
    val peers = peerData.map(_._2.client)
    val results = hashes.map(hash ⇒ resolveChannelMessage(hash, peers))

    val seq = Future.sequence(results)
    seq.transformWith {
      case Success(responses) if responses.forall(_.nonEmpty) ⇒
        Future.successful(responses.flatten)
      case _ ⇒ Future.failed(new Exception("Failed to resolve channel-messages"))
    }
  }

  def handleSignatureRequest(
    sr: SignatureRequest
  )(implicit dao: DAO): Future[Try[SignatureResponse]] = {

    futureTryWithTimeoutMetric(
      {
        dao.metrics.incrementMetric("peerApiRXSignatureRequest")

        val hashes = sr.checkpointBlock.checkpoint.edge.data.hashes
        dao.metrics.incrementMetric(
          "signatureRequestAllHashesKnown_" + hashes.forall { h =>
            dao.transactionService.lookup(h).map(_.nonEmpty).unsafeRunSync()
          }
        )

        val updated = if (sr.checkpointBlock.simpleValidation()) {
          Some(hashSign(sr.checkpointBlock.baseHash, dao.keyPair))
        } else {
          None
        }
        SignatureResponse(updated)
      },
      "handleSignatureRequest"
    )(dao.signatureExecutionContext, dao)
  }

  def simpleResolveCheckpoint(hash: String)(implicit dao: DAO): Future[Unit] = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    def innerResolve(
      peers: List[APIClient]
    )(implicit ec: ExecutionContext): Future[CheckpointCacheData] = {
      peers match {
        case activePeer :: rest =>
          val resp = activePeer
            .getNonBlocking[Option[CheckpointCacheData]](s"checkpoint/$hash", timeout = 10.seconds)
          resp.flatMap {
            case Some(ccd) => Future.successful(ccd)
            case None =>
              dao.metrics.incrementMetric("resolvePeerIncrement")
              innerResolve(rest)
          }

        case Nil =>
          Future.failed(new RuntimeException(s"Unable to resolve checkpoint hash $hash"))

      }
    }

    val resolved = wrapFutureWithMetric(
      innerResolve(dao.peerInfo.values.map(_.client).toList)(dao.edgeExecutionContext),
      "simpleResolveCheckpoint"
    )

    resolved.map { checkpointCacheData =>
      if (!dao.checkpointService.contains(checkpointCacheData.checkpointBlock.get.baseHash)) {
        dao.metrics.incrementMetric("resolveAcceptCBCall")
        acceptWithResolveAttempt(checkpointCacheData)
      }
    }

  }

  def acceptWithResolveAttempt(
    checkpointCacheData: CheckpointCacheData
  )(implicit dao: DAO): Unit = {

    dao.threadSafeSnapshotService.accept(checkpointCacheData)
    val block = checkpointCacheData.checkpointBlock.get
    val parents = block.parentSOEBaseHashes
    val parentExists = parents.map { h =>
      h -> dao.checkpointService.contains(h)
    }
    if (parentExists.forall(_._2)) {
      dao.metrics.incrementMetric("resolveFinishedCheckpointParentsPresent")
    } else {
      dao.metrics.incrementMetric("resolveFinishedCheckpointParentMissing")
      parentExists.filterNot(_._2).foreach {
        case (h, _) =>
          wrapFutureWithMetric(
            simpleResolveCheckpoint(h),
            "resolveCheckpoint"
          )(dao, dao.edgeExecutionContext)
      }

    }

  }

  def handleFinishedCheckpoint(fc: FinishedCheckpoint)(implicit dao: DAO) = {
    futureTryWithTimeoutMetric(
      if (dao.nodeState == NodeState.DownloadCompleteAwaitingFinalSync) {
        dao.threadSafeSnapshotService.syncBufferAccept(fc.checkpointCacheData)
      } else if (dao.nodeState == NodeState.Ready) {
        if (fc.checkpointCacheData.checkpointBlock.exists {
              _.simpleValidation()
            }) {
          acceptWithResolveAttempt(fc.checkpointCacheData)
        }
      },
      "handleFinishedCheckpoint"
    )(dao.finishedExecutionContext, dao)
  }

}

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int)

case class SnapshotInfo(
  snapshot: Snapshot,
  acceptedCBSinceSnapshot: Seq[String] = Seq(),
  acceptedCBSinceSnapshotCache: Seq[CheckpointCacheData] = Seq(),
  lastSnapshotHeight: Int = 0,
  snapshotHashes: Seq[String] = Seq(),
  addressCacheData: Map[String, AddressCacheData] = Map(),
  tips: Map[String, TipData] = Map(),
  snapshotCache: Seq[CheckpointCacheData] = Seq()
)

case object GetMemPool

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String]) extends Signable

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCacheData])

case class DownloadComplete(latestSnapshot: Snapshot)

import java.nio.file.{Files, Paths}

object Snapshot {

  def writeSnapshot(storedSnapshot: StoredSnapshot)(implicit dao: DAO): Try[Path] = {
    val serialized = KryoSerializer.serializeAnyRef(storedSnapshot)
    writeSnapshot(storedSnapshot, serialized)
  }

  private def writeSnapshot(storedSnapshot: StoredSnapshot,
                            serialized: Array[Byte],
                            trialNumber: Int = 0)(implicit dao: DAO): Try[Path] = {

    trialNumber match {
      case x if x >= 3 => Failure(new IOException(s"Unable to write snapshot"))
      case _ if isOverDiskCapacity(serialized.length) =>
        removeOldSnapshots()
        writeSnapshot(storedSnapshot, serialized, trialNumber + 1)
      case _ =>
        tryWithMetric(
          {
            Files.write(
              Paths.get(dao.snapshotPath.pathAsString, storedSnapshot.snapshot.hash),
              serialized
            )
          },
          "writeSnapshot"
        )
    }
  }

  def removeOldSnapshots()(implicit dao: DAO): Unit = {
    val sortedHashes = snapshotHashes().sortBy(Distance.calculate(_, dao.id))
    sortedHashes
      .slice(ConfigUtil.snapshotClosestFractionSize, sortedHashes.size)
      .foreach(snapId => Files.delete(Paths.get(dao.snapshotPath.pathAsString, snapId)))
  }

  def isOverDiskCapacity(bytesLengthToAdd: Long)(implicit dao: DAO): Boolean = {
    val storageDir = new java.io.File(dao.snapshotPath.pathAsString)
    val usableSpace = storageDir.getUsableSpace
    val occupiedSpace = dao.snapshotPath.size
    occupiedSpace + bytesLengthToAdd >= ConfigUtil.snapshotSizeDiskLimit || usableSpace <= bytesLengthToAdd
  }

  def loadSnapshot(snapshotHash: String)(implicit dao: DAO): Try[StoredSnapshot] = {
    tryWithMetric(
      {
        KryoSerializer.deserializeCast[StoredSnapshot] {
          val byteArray = Files.readAllBytes(Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        }
      },
      "loadSnapshot"
    )
  }

  def loadSnapshotBytes(snapshotHash: String)(implicit dao: DAO): Try[Array[Byte]] = {
    tryWithMetric(
      {
        val path = Paths.get(dao.snapshotPath.pathAsString, snapshotHash)
        if (Files.exists(path)) {
          val byteArray = Files.readAllBytes(path)
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        } else throw new RuntimeException(s"No snapshot found at $path")
      },
      "loadSnapshot"
    )
  }

  def snapshotHashes()(implicit dao: DAO): List[String] = {
    dao.snapshotPath.list.map { _.name }.toList
  }

  def findLatestMessageWithSnapshotHash(
    depth: Int,
    lastMessage: Option[ChannelMessageMetadata],
    maxDepth: Int = 100
  )(implicit dao: DAO): Option[ChannelMessageMetadata] = {

    def findLatestMessageWithSnapshotHashInner(
      depth: Int,
      lastMessage: Option[ChannelMessageMetadata]
    ): Option[ChannelMessageMetadata] = {
      if (depth > maxDepth) None
      else {
        lastMessage.flatMap { m =>
          if (m.snapshotHash.nonEmpty) Some(m)
          else {
            findLatestMessageWithSnapshotHashInner(
              depth + 1,
              dao.messageService.getSync(
                m.channelMessage.signedMessageData.data.previousMessageHash
              )
            )
          }
        }
      }
    }

    findLatestMessageWithSnapshotHashInner(depth, lastMessage)
  }

  def triggerSnapshot(round: Long)(implicit dao: DAO): Future[Try[Unit]] = {
    // TODO: Refactor round into InternalHeartbeat
    if (round % dao.processingConfig.snapshotInterval == 0 && dao.nodeState == NodeState.Ready) {
      futureTryWithTimeoutMetric(
        dao.threadSafeSnapshotService.attemptSnapshot(),
        "snapshotAttempt"
      )(dao.edgeExecutionContext, dao)
    } else Future.successful(Try(()))
  }

  val snapshotZero = Snapshot("", Seq())
  val snapshotZeroHash: String = Snapshot("", Seq()).hash

  def acceptSnapshot(snapshot: Snapshot)(implicit dao: DAO): Unit = {
    // dao.dbActor.putSnapshot(snapshot.hash, snapshot)
    val cbData = snapshot.checkpointBlocks.map { dao.checkpointService.get }

    if (cbData.exists { _.isEmpty }) {
      dao.metrics.incrementMetric("snapshotCBAcceptQueryFailed")
    }

    for (cbOpt <- cbData;
         cbCache <- cbOpt;
         cb <- cbCache.checkpointBlock;
         message <- cb.messages) {
      dao.messageService.updateSync(
        message.signedMessageData.hash,
        _.copy(snapshotHash = Some(snapshot.hash)),
        ChannelMessageMetadata(message, Some(cb.baseHash), Some(snapshot.hash))
      )
      dao.metrics.incrementMetric("messageSnapshotHashUpdated")
    }

    dao.checkpointService.memPool.removeSync(snapshot.checkpointBlocks.toSet)

    for (cbOpt <- cbData;
         cbCache <- cbOpt;
         cb <- cbCache.checkpointBlock;
         tx <- cb.transactions) {
      // TODO: Should really apply this to the N-1 snapshot instead of doing it directly
      // To allow consensus more time since the latest snapshot includes all data up to present, but this is simple for now
      dao.addressService
        .transferSnapshot(tx)
        .flatMap { _ ⇒
          dao.transactionService.memPool.remove(tx.hash)
          dao.acceptedTransactionService.remove(Set(tx.hash))
        }
        .flatTap(_ ⇒ dao.metrics.incrementMetricAsync("snapshotAppliedBalance"))
        .unsafeRunSync()
    }
  }

}
