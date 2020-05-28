package org.constellation.checkpoint

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.consensus.{FinishedCheckpoint, SignatureRequest, SignatureResponse}
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.checkpointBlock.{AwaitingCheckpointBlock, CheckpointBlockDoubleSpendChecker}
import org.constellation.domain.observation.{CheckpointBlockInvalid, Observation, ObservationService}
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.p2p.{Cluster, DataResolver}
import org.constellation.primitives.Schema.{CheckpointCache, Height, NodeState}
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.util.{HashSignature, Metrics, PeerApiClient}
import org.constellation.{ConstellationExecutionContext, DAO}

import scala.concurrent.duration._

class CheckpointAcceptanceService[F[_]: Concurrent: Timer](
  addressService: AddressService[F],
  blacklistedAddresses: BlacklistedAddresses[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  concurrentTipService: ConcurrentTipService[F],
  snapshotService: SnapshotService[F],
  checkpointService: CheckpointService[F],
  checkpointParentService: CheckpointParentService[F],
  checkpointBlockValidator: CheckpointBlockValidator[F],
  cluster: Cluster[F],
  rateLimiting: RateLimiting[F],
  dao: DAO
) {
  import CheckpointAcceptanceService._

  val contextShift: ContextShift[IO] =
    IO.contextShift(ConstellationExecutionContext.bounded) // TODO: wkoszycki pass from F

  val awaiting: Ref[F, Set[CheckpointCache]] = Ref.unsafe(Set())
  val pendingAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set())
  val pendingAcceptanceFromOthers: Ref[F, Set[String]] = Ref.unsafe(Set())
  val maxDepth: Int = 10

  val waitingForAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set()) // soeHash, used to check before resolving parents

  val acceptLock: Semaphore[F] = ConstellationExecutionContext.createSemaphore[F](1)

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  val dataResolver = new DataResolver(dao)

  def handleSignatureRequest(sr: SignatureRequest): F[SignatureResponse] =
    checkpointBlockValidator
      .simpleValidation(sr.checkpointBlock)
      .map(_.isValid)
      .map(valid => if (valid) Some(hashSign(sr.checkpointBlock.baseHash, dao.keyPair)) else none[HashSignature])
      .map(SignatureResponse(_))

  // TODO: For debugging only
  private def logConditions(cb: CheckpointBlock, correct: Boolean): F[Unit] =
    cb.transactions.toList
      .groupBy(_.src.address)
      .mapValues(_.sortBy(_.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            transactionService.transactionChainService
              .getLastAcceptedTransactionRef(hash)
              .map(
                la =>
                  s"${if (correct) s"${Console.GREEN}+++" else s"${Console.RED}---"}[${dao.id.short}] [${cb.baseHash}] Last accepted: ${la} | Tx ref: ${txs.headOption
                    .map(_.lastTxRef)} | Tx first: ${txs.headOption.map(a => (a.hash, a.ordinal))} | Tx last: ${txs.lastOption
                    .map(a => (a.hash, a.ordinal))} | Src: $hash | Dummy: ${txs.headOption.map(_.isDummy)} ${Console.RESET}"
              )
        }
      }
      .flatMap {
        _.traverse_(s => logger.info(s))
      }

  def acceptWithNodeCheck(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] =
    cluster.getNodeState.flatMap {
      case state if NodeState.canAcceptCheckpoint(state) =>
        logger.debug(
          s"Node (state=${state}) can accept checkpoint: ${checkpoint.checkpointCacheData.checkpointBlock.baseHash}"
        ) >> accept(
          checkpoint
        )
      case state if NodeState.canAwaitForCheckpointAcceptance(state) =>
        logger.debug(
          s"Node (state=${state}) cannot accept checkpoint, adding hash=${checkpoint.checkpointCacheData.checkpointBlock.baseHash} to sync buffer pool"
        ) >> snapshotService.syncBufferAccept(checkpoint)
      case state =>
        logger.error(
          s"Node (state=${state}) cannot accept checkpoint hash=${checkpoint.checkpointCacheData.checkpointBlock.baseHash}"
        ) >> Sync[F].raiseError[Unit](PendingDownloadException(dao.id))
    }

  def accept(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] = {
    val obtainPeers = cluster.getPeerInfo.map { allPeers =>
      val filtered = allPeers.filter(t => checkpoint.facilitators.contains(t._1))
      (if (filtered.isEmpty) allPeers else filtered)
        .map(p => PeerApiClient(p._1, p._2.peerMetadata.toPeerClientMetadata))
        .toList
    }

    val cb = checkpoint.checkpointCacheData.checkpointBlock
    val acceptance = for {
      _ <- syncPending(pendingAcceptanceFromOthers, cb.baseHash)
      _ <- checkPending(cb.baseHash)
      _ <- logger.debug(s"[${dao.id.short}] starting accept block: ${cb.baseHash} from others")
      _ <- accept(checkpoint.checkpointCacheData, checkpoint.facilitators)
      _ <- pendingAcceptanceFromOthers.modify(p => (p.filterNot(_ == cb.baseHash), ()))
    } yield ()

    acceptance.recoverWith {
      case ex: PendingAcceptance =>
        acceptErrorHandler(ex)
      case error =>
        pendingAcceptanceFromOthers.modify(p => (p.filterNot(_ == cb.baseHash), ())) >> acceptErrorHandler(error)
    }
  }

  def accept(checkpoint: CheckpointCache, facilitators: Set[Id] = Set.empty)(implicit cs: ContextShift[F]): F[Unit] = {

    val cb = checkpoint.checkpointBlock
    val acceptCheckpoint: F[Unit] =
      for {
        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Added for acceptance")
        _ <- acceptLock.acquire
        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Acquired lock")
        _ <- syncPending(pendingAcceptance, cb.baseHash)
        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking already stored")
        _ <- checkpointService
          .contains(cb.baseHash)
          .ifM(
            dao.metrics
              .incrementMetricAsync[F]("checkpointAcceptBlockAlreadyStored") >> CheckpointAcceptBlockAlreadyStored(cb)
              .raiseError[F, Unit],
            Sync[F].unit
          )

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing parents")
        _ <- AwaitingCheckpointBlock
          .areParentsSOEAccepted(checkpointParentService.soeService)(cb)
          .ifM(
            logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing parents - unit") >> Sync[F].unit,
            logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing parents - error") >>
              awaiting.modify(s => (s + checkpoint, ())) >>
              MissingParents(cb).raiseError[F, Unit]
          )

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing references")
        _ <- AwaitingCheckpointBlock
          .areReferencesAccepted(transactionService.transactionChainService)(cb)
          .ifM(
            logConditions(cb, true) >> Sync[F].unit,
            logConditions(cb, false) >>
              awaiting.modify(s => (s + checkpoint, ())) >>
              MissingTransactionReference(cb).raiseError[F, Unit]
          )

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking conflicts")
        conflicts <- LiftIO[F].liftIO(checkpointBlockValidator.containsAlreadyAcceptedTx(cb))

        _ <- conflicts match {
          case Nil => Sync[F].unit
          case xs =>
            concurrentTipService
              .putConflicting(cb.baseHash, cb)
              .flatMap(_ => transactionService.removeConflicting(xs))
              .flatMap(_ => Sync[F].raiseError[Unit](TipConflictException(cb, conflicts)))
        }

        validation <- checkpointBlockValidator.simpleValidation(cb)
        addressesWithInsufficientBalances = if (validation.isInvalid) getAddressesWithInsufficientBalances(validation)
        else List.empty

        _ <- if (validation.isInvalid)
          facilitators.toList
            .traverse(
              id =>
                observationService
                  .put(Observation.create(id, CheckpointBlockInvalid(cb.baseHash, validation))(dao.keyPair))
            )
            .flatMap(
              _ =>
                if (addressesWithInsufficientBalances.nonEmpty)
                  ContainsInvalidTransactionsException(
                    cb,
                    cb.transactions
                      .filter(t => addressesWithInsufficientBalances.contains(t.src.address))
                      .map(_.hash)
                      .toList
                  ).raiseError[F, Unit]
                else Sync[F].raiseError[Unit](new Exception(s"CB to accept not valid: $validation"))
            )
        else Sync[F].unit

        _ <- checkpointParentService.soeService
          .put(cb.soeHash, cb.soe) // TODO: consider moving down
        maybeHeight <- checkpointParentService.calculateHeight(cb).map(h => if (h.isEmpty) checkpoint.height else h)
        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Height is empty: ${maybeHeight.isEmpty}")
        height <- if (maybeHeight.isEmpty) {
          dao.metrics
            .incrementMetricAsync[F](Metrics.heightEmpty)
            .flatMap(_ => MissingHeightException(cb).raiseError[F, Height])
        } else Sync[F].pure(maybeHeight.get)

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Accept data")
        _ <- checkpointService.put(checkpoint.copy(height = maybeHeight))
        _ <- checkpointParentService.incrementChildrenCount(cb)
        _ <- Sync[F].delay(dao.recentBlockTracker.put(checkpoint.copy(height = maybeHeight)))
        _ <- acceptMessages(cb)
        doubleSpendTxs <- checkDoubleSpendTransaction(cb)
        _ <- acceptTransactions(cb, Some(checkpoint), doubleSpendTxs.map(_.hash))
        _ <- acceptObservations(cb, Some(checkpoint))
        _ <- updateRateLimiting(cb)
        _ <- Sync[F].delay {
          logger.debug(s"[${dao.id.short}] Accept checkpoint=${cb.baseHash}] and height $maybeHeight")
        }
        _ <- concurrentTipService.update(cb, height)
        _ <- snapshotService.updateAcceptedCBSinceSnapshot(cb)
        _ <- dao.metrics.incrementMetricAsync[F](Metrics.checkpointAccepted)
        _ <- incrementMetricIfDummy(cb)
        _ <- pendingAcceptance.modify(pa => (pa.filterNot(_ == cb.baseHash), ()))
        _ <- waitingForAcceptance.modify(w => (w.filterNot(_ == cb.soeHash), ()))
        _ <- acceptLock.release
        awaitingBlocks <- awaiting.modify { s =>
          val ret = s.filterNot(_.checkpointBlock.baseHash == cb.baseHash)
          (ret, ret)
        }
        allowedToAccept <- awaitingBlocks.toList.filterA { c =>
          AwaitingCheckpointBlock.areParentsSOEAccepted(checkpointParentService.soeService)(c.checkpointBlock)
        }.flatMap(_.filterA { c =>
            AwaitingCheckpointBlock.areReferencesAccepted(transactionService.transactionChainService)(c.checkpointBlock)
          })
          .flatMap(_.filterA { c =>
            AwaitingCheckpointBlock.hasNoBlacklistedTxs(c.checkpointBlock)(blacklistedAddresses)
          })

        _ <- dao.metrics.updateMetricAsync[F]("awaitingForAcceptance", awaitingBlocks.size)
        _ <- dao.metrics.updateMetricAsync[F]("allowedForAcceptance", allowedToAccept.size)
        _ <- logger.debug(
          s"Awaiting for acceptance: ${awaitingBlocks.size} | Allowed to accept: ${allowedToAccept.size}"
        )

        _ <- Concurrent[F].start(allowedToAccept.traverse(accept(_)))
      } yield ()

    acceptCheckpoint.handleErrorWith {
      case ex @ (PendingAcceptance(_) | MissingCheckpointBlockException) =>
        acceptErrorHandler(ex)
      case ex =>
        pendingAcceptance.modify { pa =>
          (pa.filterNot(_ == checkpoint.checkpointBlock.baseHash), ())
        } >> waitingForAcceptance.modify { w =>
          (w.filterNot(_ == checkpoint.checkpointBlock.soeHash), ())
        } >> acceptErrorHandler(ex)
    }
  }

  private def getAddressesWithInsufficientBalances(validation: ValidationResult[CheckpointBlock]): List[String] =
    validation
      .fold(
        _.toList.flatMap {
          case InsufficientBalance(address) => List(address)
          case _                            => List.empty
        },
        _ => List.empty
      )

  private def checkDoubleSpendTransaction(cb: CheckpointBlock): F[List[Transaction]] =
    for {
      doubleSpendTxs <- CheckpointBlockDoubleSpendChecker.check(cb)(transactionService.transactionChainService)
      _ <- if (doubleSpendTxs.nonEmpty)
        logger.info(
          s"[${dao.id.short}] CheckpointBlock with hash=${cb.baseHash} : contains double spend transactions=${doubleSpendTxs
            .map(_.hash)} : from address : ${doubleSpendTxs.map(_.src.address)}"
        ) >> dao.metrics.updateMetricAsync[F]("doubleSpendTransactions", doubleSpendTxs.size)
      else Sync[F].unit
      _ <- if (doubleSpendTxs.nonEmpty) blacklistedAddresses.addAll(doubleSpendTxs.map(_.src.address)) else Sync[F].unit
    } yield doubleSpendTxs

  private[checkpoint] def syncPending(storage: Ref[F, Set[String]], baseHash: String): F[Unit] =
    storage.modify { hashes =>
      if (hashes.contains(baseHash)) {
        throw PendingAcceptance(baseHash)
      } else {
        (hashes + baseHash, ())
      }
    }

  private[checkpoint] def checkPending(baseHash: String): F[Unit] =
    awaiting.get.map { cbs =>
      if (cbs.map(_.checkpointBlock.baseHash).contains(baseHash)) {
        throw PendingAcceptance(baseHash)
      }
    }

  def resolveMissingReferences(cb: CheckpointBlock)(implicit cs: ContextShift[F]): F[Unit] = {
    implicit val _dao = dao

    getMissingTransactionReferences(cb)(transactionService.transactionChainService).map { txs =>
      txs.flatMap(_.headOption)
    }.flatMap { txs =>
      if (txs.nonEmpty) {
        for {
//          stored <- txs.traverse(transactionService.lookup).map(_.flatten)
//          missing = txs.diff(stored.map(_.hash))
//          resolved <- LiftIO[F].liftIO(DataResolver.resolveBatchTransactionsDefaults(missing)(contextShift))
//          allTxs = stored ++ resolved
          allTxs <- LiftIO[F].liftIO(dataResolver.resolveBatchTransactionsDefaults(txs)(contextShift))
          _ <- logger.info(s"${Console.YELLOW}${allTxs.map(tx => (tx.hash, tx.cbBaseHash))}${Console.RESET}")
          cbs <- allTxs
            .flatMap(_.cbBaseHash)
            .distinct
            .filterA(checkpointService.contains(_).map(!_))
            .flatMap {
              _.filterA(hash => awaiting.get.map(_.map(_.checkpointBlock.baseHash).contains(hash)).map(!_))
            }
            .flatMap {
              _.traverse { hash =>
                LiftIO[F].liftIO(dataResolver.resolveCheckpointDefaults(hash)(contextShift))
              }
            }
          _ <- cbs.traverse(accept(_))
        } yield ()
      } else Sync[F].unit
    }
  }

  def resolveMissingParents(
    cb: CheckpointBlock,
    depth: Int = 1
  ): F[List[CheckpointCache]] =
    for {
      _ <- Sync[F].unit
      soeHashes = cb.parentSOEHashes.toList
      alreadyAcceptedSoeHashes <- soeHashes
        .traverse(checkpointParentService.soeService.lookup)
        .map(_.flatten)
        .map(_.map(_.hash))
      awaitingSoeHashes <- awaiting.get.map(_.toList.map(_.checkpointBlock.soeHash))
      waitingForAcceptanceSoeHashes <- waitingForAcceptance.get.map(_.toList)
      existing = alreadyAcceptedSoeHashes ++ awaitingSoeHashes ++ waitingForAcceptanceSoeHashes
      missingSoeHashes = soeHashes.diff(existing)

      resolveCheckpoints <- Sync[F]
        .pure(missingSoeHashes)
        .map(_.flatMap(_ => checkpointParentService.parentBaseHashesDirect(cb)))
        .flatMap {
          _.filterA(checkpointService.contains(_).map(!_))
        }
        .flatMap {
          _.filterA(hash => awaiting.get.map(_.map(_.checkpointBlock.baseHash).contains(hash)).map(!_))
        }
        .flatMap {
          _.traverse { hash =>
            LiftIO[F].liftIO(dataResolver.resolveCheckpointDefaults(hash)(contextShift)(dao = dao))
          }
        }
    } yield resolveCheckpoints

  def acceptErrorHandler(err: Throwable)(implicit cs: ContextShift[F]): F[Unit] =
    err match {
      case knownError @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][1-?] Release lock") >>
          knownError.raiseError[F, Unit]

      case error @ MissingParents(cb) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][2-${cb.baseHash}] Release lock") >>
          Concurrent[F].start(Timer[F].sleep(3.seconds) >> resolveMissingParents(cb)) >>
          Concurrent[F].start(Timer[F].sleep(20.seconds) >> resolveMissingParents(cb)) >>
          dao.metrics.incrementMetricAsync("missingParents") >> error.raiseError[F, Unit]

      case error @ MissingTransactionReference(cb) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][3-${cb.baseHash}] Release lock") >>
          Concurrent[F].start(Timer[F].sleep(3.seconds) >> resolveMissingReferences(cb)) >>
          Concurrent[F].start(Timer[F].sleep(20.seconds) >> resolveMissingReferences(cb)) >>
          error.raiseError[F, Unit]

      case otherError =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][4-?] Release lock") >>
          Sync[F].delay(logger.error(s"Error when accepting block: ${otherError.getMessage}")) >>
          dao.metrics.incrementMetricAsync[F]("acceptCheckpoint_failure") >>
          otherError.raiseError[F, Unit]
    }

  private def incrementMetricIfDummy(checkpointBlock: CheckpointBlock): F[Unit] =
    if (checkpointBlock.transactions.forall(_.isDummy)) {
      dao.metrics.incrementMetricAsync[F]("checkpointsAcceptedWithDummyTxs")
    } else {
      Sync[F].unit
    }

  def acceptObservations(cb: CheckpointBlock, cpc: Option[CheckpointCache] = None): F[Unit] =
    cb.observations.toList.traverse(o => observationService.accept(o, cpc)).void

  def acceptTransactions(
    cb: CheckpointBlock,
    cpc: Option[CheckpointCache] = None,
    txsHashToFilter: Seq[String] = Seq.empty
  ): F[Unit] = {
    def toCacheData(tx: Transaction) = TransactionCacheData(
      tx,
      Map(cb.baseHash -> true),
      cbBaseHash = Some(cb.baseHash)
    )

    cb.transactions.toList
      .filterNot(h => txsHashToFilter.contains(h.hash))
      .map(tx => (tx, toCacheData(tx)))
      .traverse {
        case (tx, txMetadata) => transactionService.accept(txMetadata, cpc) >> transfer(tx)
      }
      .void
  }

  private def transfer(tx: Transaction): F[Unit] =
    shouldTransfer(tx).ifM(
      addressService.transfer(tx).void,
      logger.debug(s"[${dao.id.short}] Transaction with hash blocked=${tx.hash} : is dummy=${tx.isDummy}")
    )

  private def shouldTransfer(tx: Transaction): F[Boolean] =
    for {
      isBlacklisted <- isBlacklistedAddress(tx)
      _ <- if (isBlacklisted) dao.metrics.incrementMetricAsync[F]("blockedBlacklistedTxs") else Sync[F].unit
      _ <- if (isBlacklisted)
        logger.info(s"[$dao.id.short] Transaction with hash=${tx.hash} : is from blacklisted address=${tx.src.address}")
      else Sync[F].unit
      isDummy = tx.isDummy
    } yield !(isBlacklisted || isDummy)

  private def isBlacklistedAddress(tx: Transaction): F[Boolean] =
    blacklistedAddresses.contains(tx.src.address)

  private def updateRateLimiting(cb: CheckpointBlock): F[Unit] =
    rateLimiting.update(cb.transactions.toList)

  private def acceptMessages(cb: CheckpointBlock): F[List[Unit]] =
    LiftIO[F].liftIO {
      cb.messages.map { m =>
        val channelMessageMetadata = ChannelMessageMetadata(m, Some(cb.baseHash))
        val messageUpdate =
          if (!m.signedMessageData.data.previousMessageHash.equals(Genesis.Coinbase)) {
            for {
              _ <- dao.messageService.memPool.put(
                m.signedMessageData.data.channelId,
                channelMessageMetadata
              )
              _ <- dao.channelService.update(
                m.signedMessageData.hash, { cmd =>
                  val slicedMessages = cmd.last25MessageHashes.slice(0, 25)
                  cmd.copy(
                    totalNumMessages = cmd.totalNumMessages + 1,
                    last25MessageHashes = Seq(m.signedMessageData.hash) ++ slicedMessages
                  )
                }
              )
            } yield ()
          } else {
            IO.unit
            // Unsafe json extract
//            dao.channelService.put(
//              m.signedMessageData.hash,
//              ChannelMetadata(
//                m.signedMessageData.data.message.x[ChannelOpen],
//                channelMessageMetadata
//              )
//            )
          }

        for {
          _ <- messageUpdate
          _ <- dao.messageService.memPool
            .put(m.signedMessageData.hash, channelMessageMetadata)
          _ <- dao.metrics.incrementMetricAsync[IO]("messageAccepted")
        } yield ()
      }.toList.sequence
    }

  def calculateHeight(cb: CheckpointBlock): F[Option[Height]] = checkpointParentService.calculateHeight(cb)
}

object CheckpointAcceptanceService {

  def isCheckpointBlockAllowedForAcceptance[F[_]: Concurrent](
    cb: CheckpointBlock
  )(txChainService: TransactionChainService[F]) =
    areTransactionsAllowedForAcceptance(cb.transactions.toList)(txChainService)

  def areTransactionsAllowedForAcceptance[F[_]: Concurrent](
    txs: List[Transaction]
  )(txChainService: TransactionChainService[F]) =
    txs
      .groupBy(_.src.address)
      .mapValues(_.sortBy(_.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            txChainService
              .getLastAcceptedTransactionRef(hash)
              .map(txs.headOption.map(_.lastTxRef).contains)
        }
      }
      .map(_.forall(_ == true))

  def getMissingTransactionReferences[F[_]: Concurrent](cb: CheckpointBlock)(
    txChainService: TransactionChainService[F]
  ): F[List[List[String]]] = // mwadon: List[List because of grouping by addresses
    cb.transactions.toList
      .groupBy(_.src.address)
      .mapValues(_.sortBy(_.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            txChainService
              .getLastAcceptedTransactionRef(hash)
              .map(a => {
                if (txs.headOption.map(_.lastTxRef).contains(a)) {
                  List.empty
                } else txs.map(_.lastTxRef.prevHash).filterNot(_ == "")
              })
        }
      }
}
