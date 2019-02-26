package org.constellation.consensus

import java.nio.file.Path

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Validation.EnrichedFuture
import org.constellation.util.{APIClient, HashSignature, Signable}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Success, Try}

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

      cb.checkpoint.edge.data.messages.foreach { m =>
        if (m.signedMessageData.data.previousMessageHash != Genesis.CoinBaseHash) {
          dao.messageService.put(
            m.signedMessageData.data.channelId,
            ChannelMessageMetadata(m, Some(cb.baseHash))
          )
          dao.channelService.updateOnly(
            m.signedMessageData.hash,
            { cmd =>
              val slicedMessages = if (cmd.last25MessageHashes.size > 25) {
                cmd.last25MessageHashes.slice(0, 24)
              } else cmd.last25MessageHashes
              cmd.copy(
                totalNumMessages = cmd.totalNumMessages + 1,
                last25MessageHashes = slicedMessages :+ m.signedMessageData.hash
              )
            }
          )
        } else { // Unsafe json extract
          dao.channelService.put(
            m.signedMessageData.hash,
            ChannelMetadata(
              m.signedMessageData.data.message.x[ChannelOpen],
              ChannelMessageMetadata(m, Some(cb.baseHash))
            )
          )
        }
        dao.messageService.put(m.signedMessageData.hash,
                               ChannelMessageMetadata(m, Some(cb.baseHash)))
        dao.metrics.incrementMetric("messageAccepted")
      }

      // Accept transactions
      cb.transactions.foreach { t =>
        dao.metrics.incrementMetric("transactionAccepted")
        t.store(
          TransactionCacheData(
            t,
            valid = true,
            inMemPool = false,
            inDAG = true,
            Map(cb.baseHash -> true),
            cbBaseHash = Some(cb.baseHash)
          )
        )
        t.ledgerApply()
      }
      dao.metrics.incrementMetric("checkpointAccepted")
      val data = CheckpointCacheData(
        Some(cb),
        height = fallbackHeight
      )
      cb.store(
        data
      )

      dao.recentBlockTracker.put(data)

    }
  }

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

  // TODO: Move to checkpoint formation actor

  def formCheckpoint(messages: Seq[ChannelMessage] = Seq())(
    implicit dao: DAO
  ) = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    val maybeTransactions =
      dao.threadSafeTXMemPool.pull(dao.minCheckpointFormationThreshold)

    dao.metrics.incrementMetric("attemptFormCheckpointCalls")

    if (maybeTransactions.isEmpty) {
      dao.metrics.incrementMetric("attemptFormCheckpointInsufficientTX")
    }

    def requestBlockSignature(
      checkpointBlock: CheckpointBlock,
      finalFacilitators: Set[
        Id
      ],
      data: PeerData
    ) = {
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
    def processSignedBlock(
      cache: CheckpointCacheData,
      finalFacilitators: Set[
        Id
      ]
    ) = {

      val responses = dao.peerInfo.values.toList.map { peer =>
        wrapFutureWithMetric(
          peer.client.postNonBlocking[Option[FinishedCheckpointResponse]](
            "finished/checkpoint",
            FinishedCheckpoint(cache, finalFacilitators),
            timeout = 20.seconds
          ),
          "finishedCheckpointBroadcast",
        )
      }

      responses.traverse(_.toValidatedNel).map(_.sequence)
    }

    val result = maybeTransactions.flatMap { transactions =>
      val maybeTips = dao.threadSafeTipService.pull()
      if (maybeTips.isEmpty) {
        dao.metrics.incrementMetric("attemptFormCheckpointInsufficientTipsOrFacilitators")
        if (dao.nodeConfig.isGenesisNode) {
          val maybeTips = dao.threadSafeTipService.pull(allowEmptyFacilitators = true)
          if (maybeTips.isEmpty) {
            dao.metrics.incrementMetric("attemptFormCheckpointNoGenesisTips")
          }
          maybeTips.foreach {
            case (tipSOE, _) =>
              val checkpointBlock =
                CheckpointBlock.createCheckpointBlock(transactions, tipSOE.map { soe =>
                  TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)
                }, messages)(dao.keyPair)

              val cache =
                CheckpointCacheData(
                  Some(checkpointBlock),
                  height = checkpointBlock.calculateHeight()
                )

              dao.threadSafeTipService.accept(cache)
              dao.threadSafeMessageMemPool.release(messages)

          }
          dao.blockFormationInProgress = false
        }

      }

      maybeTips.map {
        case (tipSOE, facils) =>
          // Change to method on TipsReturned // abstract for reuse.
          val checkpointBlock = CheckpointBlock.createCheckpointBlock(transactions, tipSOE.map {
            soe =>
              TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)
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
                  dao.threadSafeTipService.accept(cache)
                  processSignedBlock(cache, finalFacilitators)
                }
                .map {
                  _.andThen(identity)
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
    }
    dao.blockFormationInProgress = false
    result.sequence
  }

  def resolveTransactions(
    hashes: Seq[String],
    priorityPeer: Id
  )(implicit dao: DAO): Future[Seq[TransactionCacheData]] = {

    implicit val exec: ExecutionContextExecutor = dao.signatureExecutionContext

    def lookupTransaction(hash: String, client: APIClient): Future[Option[TransactionCacheData]] = {
      async {
        await(
          client.getNonBlocking[Option[TransactionCacheData]](
            s"transaction/$hash",
            timeout = 4.seconds
          )
        )
      }
    }

    def resolveTransaction(hash: String,
                           peers: Seq[APIClient]): Future[Option[TransactionCacheData]] = {

      var remainingPeers = peers.tail

      lookupTransaction(hash, peers.head).transformWith {
        case Success(Some(transactionCacheData)) => Future.successful(Some(transactionCacheData))
        case _ =>
          remainingPeers.headOption
            .map { peer =>
              remainingPeers = remainingPeers.filterNot(_ == peer)
              lookupTransaction(hash, peer)
            }
            .getOrElse(Future.failed(new Exception("Ran out of peers to query for transaction")))
      }

    }

    // Change to facilitator ordering
    val peers = dao.readyPeers.toSeq.sortBy { _._1 != priorityPeer }.map { _._2.client }
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

  def handleSignatureRequest(
    sr: SignatureRequest
  )(implicit dao: DAO): Future[Try[SignatureResponse]] = {

    futureTryWithTimeoutMetric(
      {
        dao.metrics.incrementMetric("peerApiRXSignatureRequest")

        val hashes = sr.checkpointBlock.checkpoint.edge.data.hashes
        dao.metrics.incrementMetric(
          "signatureRequestAllHashesKnown_" + hashes.forall { h =>
            dao.transactionService.get(h).nonEmpty
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

  def simpleResolveCheckpoint(hash: String)(implicit dao: DAO): Future[Boolean] = {

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
      true
    }

  }

  def acceptWithResolveAttempt(
    checkpointCacheData: CheckpointCacheData
  )(implicit dao: DAO): Unit = {

    dao.threadSafeTipService.accept(checkpointCacheData)
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
        dao.threadSafeTipService.syncBufferAccept(fc.checkpointCacheData)
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

  def writeSnapshot(snapshot: StoredSnapshot)(implicit dao: DAO): Try[Path] = {
    tryWithMetric(
      {
        val serialized = KryoSerializer.serializeAnyRef(snapshot)
        Files.write(Paths.get(dao.snapshotPath.pathAsString, snapshot.snapshot.hash), serialized)
        //File(dao.snapshotPath, snapshot.snapshot.hash).writeByteArray(serialized)
      },
      "writeSnapshot"
    )
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
        {
          val byteArray = Files.readAllBytes(Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        }
      },
      "loadSnapshot"
    )
  }

  def snapshotHashes()(implicit dao: DAO): List[String] = {
    dao.snapshotPath.toJava.listFiles().map { _.getName }.toList
  }

  def findLatestMessageWithSnapshotHash(
    depth: Int,
    lastMessage: Option[ChannelMessageMetadata],
    maxDepth: Int = 10
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
              dao.messageService.get(
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
        dao.threadSafeTipService.attemptSnapshot(),
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
         message <- cb.checkpoint.edge.data.messages) {
      dao.messageService.update(
        message.signedMessageData.signatures.hash,
        _.copy(snapshotHash = Some(snapshot.hash)),
        ChannelMessageMetadata(message, Some(cb.baseHash), Some(snapshot.hash))
      )
      dao.metrics.incrementMetric("messageSnapshotHashUpdated")
    }

    for (cbOpt <- cbData;
         cbCache <- cbOpt;
         cb <- cbCache.checkpointBlock;
         tx <- cb.transactions) {
      // TODO: Should really apply this to the N-1 snapshot instead of doing it directly
      // To allow consensus more time since the latest snapshot includes all data up to present, but this is simple for now
      tx.ledgerApplySnapshot()
      dao.acceptedTransactionService.delete(Set(tx.hash))
      dao.metrics.incrementMetric("snapshotAppliedBalance")
    }
  }

}
