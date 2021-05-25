package org.constellation.checkpoint

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Clock, Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.syntax.all._
import constellation._
import io.chrisdavenport.log4cats.{Logger, SelfAwareStructuredLogger}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.checkpointBlock.{AwaitingCheckpointBlock, CheckpointBlockDoubleSpendChecker, CheckpointStorageAlgebra}
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.genesis.Genesis
import org.constellation.p2p.{Cluster, DataResolver, PeerData}
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, CheckpointCacheMetadata, FinishedCheckpoint, TipData}
import org.constellation.schema.edge.SignedObservationEdge
import org.constellation.schema.observation.{CheckpointBlockInvalid, Observation}
import org.constellation.schema.signature.{HashSignature, SignatureRequest, SignatureResponse}
import org.constellation.schema.transaction.{Transaction, TransactionCacheData}
import org.constellation.schema.{ChannelMessageMetadata, Height, Id, NodeState, checkpoint}
import org.constellation.storage.algebra.Lookup
import org.constellation.storage._
import org.constellation.util.Metrics
import org.constellation._
import org.constellation.concurrency.SingleLock
import org.constellation.consensus.FacilitatorFilter
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.util.Logging.logThread

import java.security.KeyPair
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class CheckpointService[F[_]: Concurrent: Timer: Clock](
                                                         merkleService: CheckpointMerkleService[F],
                                                         addressService: AddressService[F],
                                                         blacklistedAddresses: BlacklistedAddresses[F],
                                                         transactionService: TransactionService[F],
                                                         observationService: ObservationService[F],
                                                         snapshotService: SnapshotService[F],
                                                         checkpointBlockValidator: CheckpointBlockValidator[F],
                                                         nodeStorage: NodeStorageAlgebra[F],
                                                         checkpointStorage: CheckpointStorageAlgebra[F],
                                                         rateLimiting: RateLimiting[F],
                                                         dataResolver: DataResolver[F],
                                                         boundedExecutionContext: ExecutionContext,
                                                         sizeLimit: Int,
                                                         maxWidth: Int,
                                                         maxTipUsage: Int,
                                                         numFacilitatorPeers: Int,
                                                         facilitatorFilter: FacilitatorFilter[F],
                                                         id: Id,
                                                         metrics: Metrics,
                                                         keyPair: KeyPair,
                                                         recentBlockTracker: RecentDataTracker[CheckpointCache]
)(C: ContextShift[F]) {

  implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  /*** checkpoint service ***/

  def fetchBatchTransactions(merkleRoot: String): F[List[Transaction]] =
    merkleService.fetchBatchTransactions(merkleRoot)

  /*** checkpoint parent service ***/


  /*** checkpoint acceptance service ***/

  import CheckpointService._

  val acceptLock: Semaphore[F] = ConstellationExecutionContext.createSemaphore[F](1)

  def handleSignatureRequest(sr: SignatureRequest): F[SignatureResponse] =
    checkpointBlockValidator
      .simpleValidation(sr.checkpointBlock)
      .map(_.isValid)
      .map(valid => if (valid) Some(hashSign(sr.checkpointBlock.baseHash, keyPair)) else none[HashSignature])
      .map(SignatureResponse(_))


  def acceptWithNodeCheck(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] =
    nodeStorage.getNodeState.flatMap {
      case state if NodeState.canAcceptCheckpoint(state) =>
        logger.debug(
          s"Node (state=${state}) can accept checkpoint: ${checkpoint.checkpointCacheData.checkpointBlock.baseHash}"
        ) >> accept(checkpoint)
      case state if NodeState.canAwaitForCheckpointAcceptance(state) =>
        logger.debug(
          s"Node (state=${state}) cannot accept checkpoint, adding hash=${checkpoint.checkpointCacheData.checkpointBlock.baseHash} to sync buffer pool"
        ) >> snapshotService.syncBufferAccept(checkpoint)
      case state =>
        logger.warn(
          s"Node (state=${state}) cannot accept checkpoint hash=${checkpoint.checkpointCacheData.checkpointBlock.baseHash}"
        ) >> Sync[F].raiseError[Unit](PendingDownloadException(id))
    }

  def accept(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] = {
    val cb = checkpoint.checkpointCacheData.checkpointBlock
    val acceptance = for {
      inAcceptance <- checkpointStorage.isCheckpointInAcceptance(cb.soeHash)
      waitingForAcceptance <- checkpointStorage.isCheckpointWaitingForAcceptance(cb.soeHash)
      // TODO: maybe it should be an atomic operation
      _ <- if (inAcceptance || waitingForAcceptance) {
        Sync[F].raiseError[Unit](PendingAcceptance(cb.soeHash))
      } else Sync[F].unit

      _ <- logger.debug(s"[${id.short}] starting accept block: ${cb.baseHash} from others")

      _ <- accept(checkpoint.checkpointCacheData, checkpoint.facilitators)
    } yield ()

    acceptance.recoverWith {
      case ex: PendingAcceptance =>
        acceptErrorHandler(ex)
      case error =>
        checkpointStorage.unmarkFromAcceptance(cb.soeHash) >> acceptErrorHandler(error)
    }
  }

  def accept(checkpoint: CheckpointCache, facilitators: Set[Id] = Set.empty)(implicit cs: ContextShift[F]): F[Unit] = {

    val cb = checkpoint.checkpointBlock
    val acceptCheckpoint: F[Unit] =
      for {
        _ <- logger.debug(s"[Accept checkpoint][${cb.soeHash}] Added for acceptance")
        _ <- registerUsages(checkpoint)
        _ <- acceptLock.acquire
        _ <- logger.debug(s"[Accept checkpoint][${cb.soeHash}] Acquired lock")
        inAcceptance <- checkpointStorage.isCheckpointInAcceptance(cb.soeHash)
        waitingForAcceptance <- checkpointStorage.isCheckpointWaitingForAcceptance(cb.soeHash)
        // TODO: maybe it should be an atomic operation
        _ <- if (inAcceptance || waitingForAcceptance) {
          Sync[F].raiseError[Unit](PendingAcceptance(cb.soeHash))
        } else Sync[F].unit
        isAccepted <- checkpointStorage.isCheckpointAccepted(cb.soeHash)
        _ <- logger.debug(s"[Accept checkpoint][${cb.soeHash}] Checking already stored")
        _ <- if (isAccepted) {
          metrics
            .incrementMetricAsync[F]("checkpointAcceptBlockAlreadyStored") >> CheckpointAcceptBlockAlreadyStored(cb)
            .raiseError[F, Unit]
        } else Sync[F].unit

        _ <- checkpointStorage.markForAcceptance(cb.soeHash)

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing parents")
        areParentsAccepted <- checkpointStorage.areParentsAccepted(checkpoint)
        _ <- if (areParentsAccepted) {
          logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing parents - unit") >>
            Sync[F].unit
        } else {
          logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing parents - error") >>
            checkpointStorage.markAsAwaiting(cb.soeHash) >>
            MissingParents(cb).raiseError[F, Unit]
        }

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking missing references")
        _ <- AwaitingCheckpointBlock.areReferencesAccepted(checkpointBlockValidator)(cb)
          .ifM(
            Sync[F].unit,
              checkpointStorage.markAsAwaiting(cb.soeHash) >>
              MissingTransactionReference(cb).raiseError[F, Unit]
          )

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Checking conflicts")
        conflicts <- checkpointBlockValidator.containsAlreadyAcceptedTx(cb)

        _ <- conflicts match {
          case Nil => Sync[F].unit
          case xs =>
            putConflictingTips(cb.baseHash, cb)
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
                  .put(Observation.create(id, CheckpointBlockInvalid(cb.baseHash, validation.toString))(keyPair))
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

        maybeHeight <- checkpointStorage.calculateHeight(cb.soeHash).map(h => if (h.isEmpty) checkpoint.height else h)
        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Height is empty: ${maybeHeight.isEmpty}")
        height <- if (maybeHeight.isEmpty) {
          metrics
            .incrementMetricAsync[F](Metrics.heightEmpty)
            .flatMap(_ => MissingHeightException(cb).raiseError[F, Height])
        } else Sync[F].pure(maybeHeight.get)

        lastSnapshotHeight <- snapshotService.getLastSnapshotHeight
        _ <- if (height.min <= lastSnapshotHeight.toLong) {
          metrics.incrementMetricAsync[F](Metrics.heightBelow) >>
            HeightBelow(checkpoint.checkpointBlock, height).raiseError[F, Unit]
        } else Sync[F].unit

        _ <- logger.debug(s"[Accept checkpoint][${cb.baseHash}] Accept data")

        _ <- checkpointStorage.acceptCheckpoint(cb.soeHash, maybeHeight)
        doubleSpendTxs <- checkDoubleSpendTransaction(cb)
        _ <- acceptTransactions(cb, Some(checkpoint), doubleSpendTxs.map(_.hash))
        _ <- acceptObservations(cb, Some(checkpoint))
        _ <- updateRateLimiting(cb)
        _ <- transactionService.findAndRemoveInvalidPendingTxs()
        _ <- logger.debug(s"[${id.short}] Accept checkpoint=${cb.baseHash}] and height $maybeHeight")
        _ <- updateTips(cb, height) // TODO: @mwadon
        _ <- snapshotService.updateAcceptedCBSinceSnapshot(cb)
        _ <- metrics.incrementMetricAsync[F](Metrics.checkpointAccepted)
        _ <- acceptLock.release
        awaitingBlocks <- checkpointStorage.getAwaiting.flatMap(_.toList.traverse(checkpointStorage.getCheckpoint).map(_.flatten))
        allowedToAccept <- awaitingBlocks.filterA { c =>
          checkpointStorage.areParentsAccepted(c)
        }.flatMap(_.filterA { c =>
          AwaitingCheckpointBlock.areReferencesAccepted(checkpointBlockValidator)(c.checkpointBlock)
        })
          .flatMap(_.filterA { c =>
            AwaitingCheckpointBlock.hasNoBlacklistedTxs(c.checkpointBlock)(blacklistedAddresses)
          })

        _ <- metrics.updateMetricAsync[F]("awaitingForAcceptance", awaitingBlocks.size)
        _ <- metrics.updateMetricAsync[F]("allowedForAcceptance", allowedToAccept.size)
        _ <- logger.debug(
          s"Awaiting for acceptance: ${awaitingBlocks.size} | Allowed to accept: ${allowedToAccept.size}"
        )

        _ <- Concurrent[F].start(
          allowedToAccept.traverse(
            c => cs.evalOn(boundedExecutionContext)(accept(c)).handleErrorWith(_ => Sync[F].unit)
          )
        )
      } yield ()

    acceptCheckpoint.handleErrorWith {
      case ex @ (PendingAcceptance(_)) =>
        acceptErrorHandler(ex)
      case ex =>
        checkpointStorage.unmarkFromAcceptance(cb.soeHash) >>
          acceptErrorHandler(ex)
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
          s"[${id.short}] CheckpointBlock with hash=${cb.baseHash} : contains double spend transactions=${doubleSpendTxs
            .map(_.hash)} : from address : ${doubleSpendTxs.map(_.src.address)}"
        ) >> metrics.updateMetricAsync[F]("doubleSpendTransactions", doubleSpendTxs.size)
      else Sync[F].unit
      _ <- if (doubleSpendTxs.nonEmpty) blacklistedAddresses.addAll(doubleSpendTxs.map(_.src.address)) else Sync[F].unit
    } yield doubleSpendTxs

  def resolveMissingReferences(cb: CheckpointBlock)(implicit cs: ContextShift[F]): F[Unit] = {
    getMissingTransactionReferences(cb)(transactionService.transactionChainService).map { txs =>
      txs.flatMap(_.headOption)
    }.flatMap { txs =>
      if (txs.nonEmpty) {
        for {
          allTxs <- dataResolver.resolveBatchTransactionsDefaults(txs)
          _ <- logger.info(s"${Console.YELLOW}${allTxs.map(tx => (tx.hash, tx.cbBaseHash))}${Console.RESET}")
          cbs <- allTxs
            .flatMap(_.cbBaseHash)
            .distinct
            .filterA(checkpointStorage.existsCheckpoint(_).map(!_))
            .flatMap {
              _.filterA(soeHash => checkpointStorage.isCheckpointAwaiting(soeHash).map(!_))
            }
            .flatMap {
              _.filterA(soeHash => checkpointStorage.isCheckpointWaitingForAcceptance(soeHash).map(!_))
            }
            .flatMap { // TODO: @mwadon - probably not needed
              _.filterA(soeHash => checkpointStorage.isCheckpointInAcceptance(soeHash).map(!_))
            }
            .flatMap(_.traverse(hash => dataResolver.resolveCheckpointDefaults(hash)))
          _ <- cbs.traverse(C.shift >> accept(_).handleErrorWith(_ => Sync[F].unit))
        } yield ()
      } else Sync[F].unit
    }
  }

  def resolveMissingParents(
                             cb: CheckpointBlock,
                             depth: Int = 1
                           ): F[List[CheckpointCache]] =
    for {
      soeHashes <- cb.parentSOEHashes.toList.pure[F]
      existing <- soeHashes.filterA(checkpointStorage.existsCheckpoint)
      missingSoeHashes = soeHashes.diff(existing)
      cbs <- missingSoeHashes.traverse(dataResolver.resolveCheckpointDefaults(_))
      _ <- cbs.traverse(C.shift >> accept(_)(C).handleErrorWith(_ => Sync[F].unit))
    } yield cbs

  def acceptErrorHandler(err: Throwable)(implicit cs: ContextShift[F]): F[Unit] =
    err match {
      case knownError @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][1-?] Release lock") >>
          knownError.raiseError[F, Unit]

      case error @ HeightBelow(hash, _) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][2-${hash}] Release lock") >>
          error.raiseError[F, Unit]

      case error @ MissingParents(cb) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][3-${cb.baseHash}] Release lock") >>
          Concurrent[F].start(resolveMissingParents(cb)) >>
          metrics.incrementMetricAsync("missingParents") >>
          error.raiseError[F, Unit]

      case error @ MissingTransactionReference(cb) =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][4-${cb.baseHash}] Release lock") >>
          Concurrent[F].start(resolveMissingReferences(cb)) >>
          error.raiseError[F, Unit]

      case otherError =>
        acceptLock.release >>
          logger.debug(s"[Accept checkpoint][5-?] Release lock") >>
          logger.error(otherError)(s"Error when accepting block") >>
          metrics.incrementMetricAsync[F]("acceptCheckpoint_failure") >>
          otherError.raiseError[F, Unit]
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
      .groupBy(_.src.address)
      .mapValues(_.sortBy(_.ordinal))
      .values
      .flatten
      .toList
      .map(tx => (tx, toCacheData(tx)))
      .traverse { case (tx, txMetadata) => transactionService.accept(txMetadata, cpc) >> transfer(tx) }
      .void
  }

  private def transfer(tx: Transaction): F[Unit] =
    shouldTransfer(tx).ifM(
      addressService.transferTransaction(tx).void,
      logger.debug(s"[${id.short}] Transaction with hash blocked=${tx.hash} : is dummy=${tx.isDummy}")
    )

  private def shouldTransfer(tx: Transaction): F[Boolean] =
    for {
      isBlacklisted <- isBlacklistedAddress(tx)
      _ <- if (isBlacklisted) metrics.incrementMetricAsync[F]("blockedBlacklistedTxs") else Sync[F].unit
      _ <- if (isBlacklisted)
        logger.info(s"[$id.short] Transaction with hash=${tx.hash} : is from blacklisted address=${tx.src.address}")
      else Sync[F].unit
      isDummy = tx.isDummy
    } yield !(isBlacklisted || isDummy)

  private def isBlacklistedAddress(tx: Transaction): F[Boolean] =
    blacklistedAddresses.contains(tx.src.address)

  private def updateRateLimiting(cb: CheckpointBlock): F[Unit] =
    rateLimiting.update(cb.transactions.toList)

  /*** concurrent tip service ***/

  private val snapshotHeightInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  private val conflictingTips: Ref[F, Map[String, CheckpointBlock]] = Ref.unsafe(Map.empty)
  private val tipsRef: Ref[F, Map[String, TipData]] = Ref.unsafe(Map.empty)
  private val semaphoreTips: Semaphore[F] = createSemaphore()
  private val usagesTips: Ref[F, Map[String, Set[String]]] = Ref.unsafe(Map.empty)

  def clearStaleTips(min: Long): F[Unit] =
    (min > snapshotHeightInterval)
      .pure[F]
      .ifM(
        tipsRef.get.map(tips => tips.filter(_._2.height.min <= min)).flatMap { toRemove =>
          logger
            .debug(
              s"Removing tips that are below cluster height: $min to remove ${toRemove.map(t => (t._1, t._2.height))}"
            )
            .flatMap(_ => tipsRef.modify(curr => (curr -- toRemove.keySet, ())))
        },
        logger.debug(
          s"Min=${min} is lower or equal snapshotHeightInterval=${snapshotHeightInterval}. Skipping tips removal"
        )
      )

  def setTips(newTips: Map[String, TipData]): F[Unit] =
    tipsRef.modify(_ => (newTips, ()))

  def tipsToMap: F[Map[String, TipData]] =
    tipsRef.get

  def markTipAsConflict(key: String)(implicit metrics: Metrics): F[Unit] =
    logThread(
      getTips(key).flatMap { m =>
        if (m.isDefined)
          removeTips(key)
            .flatMap(_ => conflictingTips.modify(c => (c + (key -> m.get.checkpointBlock), ())))
            .flatTap(_ => logger.warn(s"Marking tip as conflicted tipHash: $key"))
        else Sync[F].unit
      },
      "concurrentTipService_markAsConflict"
    )

  def updateTips(checkpointBlock: CheckpointBlock, height: Height, isGenesis: Boolean = false): F[Unit] =
    countUsages(checkpointBlock.soeHash) >>= { usages =>
      withLockTips("updateTips", updateUnsafeTips(checkpointBlock, height, usages, isGenesis))
    }

  private def withLockTips[R](name: String, thunk: F[R]) = new SingleLock[F, R](name, semaphoreTips).use(thunk)

  def updateUnsafeTips(
                        checkpointBlock: CheckpointBlock,
                        height: Height,
                        usages: Int,
                        isGenesis: Boolean = false
                      ): F[Unit] = {
    val tipUpdates = parentSOEBaseHashes(checkpointBlock)
      .flatMap(
        l =>
          l.distinct.traverse { h =>
            for {
              tipData <- getTips(h)
              size <- sizeTips
              reuseTips = size < maxWidth
              aboveMinimumTip = size >= numFacilitatorPeers
              _ <- tipData match {
                case None => Sync[F].unit
                case Some(TipData(block, numUses, _)) if aboveMinimumTip && (numUses >= maxTipUsage || !reuseTips) =>
                  removeTips(block.baseHash)(metrics)
                case Some(TipData(block, numUses, tipHeight)) =>
                  putUnsafeTips(block.baseHash, checkpoint.TipData(block, numUses + 1, tipHeight))(metrics)
                    .flatMap(_ => metrics.incrementMetricAsync("checkpointTipsIncremented"))
              }
            } yield ()
          }
      )

    logThread(
      tipUpdates
        .flatMap(_ => getMinTipHeight(None))
        .flatMap(
          min =>
            if (isGenesis || min < height.min || usages < maxTipUsage)
              putUnsafeTips(checkpointBlock.baseHash, TipData(checkpointBlock, usages, height))(metrics)
            else
              logger.debug(
                s"Block height: ${height.min} with usages=${usages} above the limit or below min tip: $min | update skipped"
              )
        )
        .recoverWith {
          case err: TipThresholdException =>
            metrics
              .incrementMetricAsync("memoryExceeded_thresholdMetCheckpoints")
              .flatMap(_ => sizeTips)
              .flatMap(s => metrics.updateMetricAsync("activeTips", s))
              .flatMap(_ => Sync[F].raiseError[Unit](err))
        },
      "concurrentTipService_updateUnsafe"
    )
  }

  def getTips(key: String): F[Option[TipData]] =
    tipsRef.get.map(_.get(key))

  def removeTips(key: String)(implicit metrics: Metrics): F[Unit] =
    tipsRef.modify(t => (t - key, ())).flatTap(_ => metrics.incrementMetricAsync("checkpointTipsRemoved"))

  private def putUnsafeTips(k: String, v: TipData)(implicit metrics: Metrics): F[Unit] =
    sizeTips.flatMap(
      size =>
        if (size < sizeLimit) tipsRef.modify(curr => (curr + (k -> v), ()))
        else Sync[F].raiseError[Unit](TipThresholdException(v.checkpointBlock, sizeLimit))
    )

  def sizeTips: F[Int] =
    tipsRef.get.map(_.size)

  def getMinTipHeight(minActiveTipHeight: Option[Long]): F[Long] =
    logThread(
      for {
        _ <- logger.debug(s"Active tip height: $minActiveTipHeight")
        keys <- tipsRef.get.map(_.keys.toList)
        maybeData <- keys.traverse(lookupCheckpoint)
        waitingHeights <- awaiting.get.map(_.flatMap(_.height.map(_.min)).toList)

        diff = keys.diff(maybeData.flatMap(_.map(_.checkpointBlock.baseHash)))
        _ <- if (diff.nonEmpty) logger.debug(s"wkoszycki not_mapped ${diff}") else Sync[F].unit

        heights = maybeData.flatMap {
          _.flatMap {
            _.height.map {
              _.min
            }
          }
        } ++ minActiveTipHeight.toList ++ waitingHeights
        minHeight = if (heights.isEmpty) 0 else heights.min
      } yield minHeight,
      "concurrentTipService_getMinTipHeight"
    )

  def countUsages(soeHash: String): F[Int] =
    usagesTips.get.map(_.get(soeHash).map(_.size).getOrElse(0))

  def putConflictingTips(k: String, v: CheckpointBlock): F[Unit] = {
    val unsafePut = for {
      size <- conflictingTips.get.map(_.size)
      _ <- metrics.updateMetricAsync("conflictingTips", size)
      _ <- conflictingTips.modify(c => (c + (k -> v), ()))
    } yield ()

    logThread(withLockTips("conflictingPut", unsafePut), "concurrentTipService_putConflicting")
  }

  def pullTips(readyFacilitators: Map[Id, PeerData])(implicit metrics: Metrics): F[Option[PulledTips]] =
    logThread(
      tipsRef.get.flatMap { tips =>
        metrics.updateMetric("activeTips", tips.size)
        (tips.size, readyFacilitators) match {
          case (size, facilitators) if size >= numFacilitatorPeers && facilitators.nonEmpty =>
            calculateTipsSOE(tips).flatMap { tipSOE =>
              facilitatorFilter.filterPeers(facilitators, numFacilitatorPeers, tipSOE).map {
                case f if f.size >= numFacilitatorPeers =>
                  Some(PulledTips(tipSOE, calculateFinalFacilitators(f, tipSOE.soe.map(_.hash).reduce(_ + _))))
                case _ => None
              }
            }
          case (size, _) if size >= numFacilitatorPeers =>
            calculateTipsSOE(tips).map(t => Some(PulledTips(t, Map.empty[Id, PeerData])))
          case (_, _) => none[PulledTips].pure[F]
        }
      },
      "concurrentTipService_pull"
    )

  private def calculateTipsSOE(tips: Map[String, TipData]): F[TipSoe] =
    Random
      .shuffle(tips.toSeq.sortBy(_._2.height.min).take(10))
      .take(numFacilitatorPeers)
      .toList
      .traverse { t =>
        calculateHeight(t._2.checkpointBlock)
          .map(h => (h, t._2.checkpointBlock.checkpoint.edge.signedObservationEdge))
      }
      .map(_.sortBy(_._2.hash))
      .map(r => TipSoe(r.map(_._2), r.map(_._1.map(_.min)).min))

  private def calculateFinalFacilitators(facilitators: Map[Id, PeerData], mergedTipHash: String): Map[Id, PeerData] = {
    // TODO: Use XOR distance instead as it handles peer data mismatch cases better
    val facilitatorIndex = (BigInt(mergedTipHash, 16) % facilitators.size).toInt
    val sortedFacils = facilitators.toSeq.sortBy(_._1.hex)
    val selectedFacils = Seq
      .tabulate(numFacilitatorPeers) { i =>
        (i + facilitatorIndex) % facilitators.size
      }
      .map {
        sortedFacils(_)
      }
    selectedFacils.toMap
  }

  def registerUsages(checkpoint: FinishedCheckpoint): F[Unit] =
    registerUsages(checkpoint.checkpointCacheData)

  def registerUsages(checkpoint: CheckpointCache): F[Unit] = {
    val parents = checkpoint.checkpointBlock.parentSOEHashes.distinct.toList
    val hash = checkpoint.checkpointBlock.soeHash

    parents.traverse { parent =>
      usagesTips.modify { m =>
        val data = m.get(parent)
        val updated = data.map(_ ++ Set(hash)).getOrElse(Set(hash))
        (m.updated(parent, updated), ())
      }
    }.void
  }

  def getUsages: F[Map[String, Set[String]]] =
    usagesTips.get

  def setUsages(u: Map[String, Set[String]]): F[Unit] =
    usagesTips.set(u)

  def batchRemoveUsages(cbs: Set[String]): F[Unit] =
    usagesTips.modify { u =>
      (u -- cbs, ())
    }


}

object CheckpointService {

  def isCheckpointBlockAllowedForAcceptance[F[_]: Concurrent](
                                                               cb: CheckpointBlock
                                                             )(checkpointBlockValidator: CheckpointBlockValidator[F]) =
    areTransactionsAllowedForAcceptance(cb.transactions.toList)(checkpointBlockValidator)

  def areTransactionsAllowedForAcceptance[F[_]: Concurrent](
                                                             txs: List[Transaction]
                                                           )(checkpointBlockValidator: CheckpointBlockValidator[F]): F[Boolean] =
    checkpointBlockValidator.validateLastTxRefChain(txs).map(_.isValid)

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
