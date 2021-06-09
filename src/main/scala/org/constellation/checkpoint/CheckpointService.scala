package org.constellation.checkpoint

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Clock, Concurrent, ContextShift, Sync, Timer}
import cats.syntax.all._
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation._
import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.consensus.FacilitatorFilter
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.checkpointBlock.{AwaitingCheckpointBlock, CheckpointBlockDoubleSpendChecker, CheckpointStorageAlgebra}
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.p2p.{DataResolver, PeerData}
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, FinishedCheckpoint}
import org.constellation.schema.signature.{HashSignature, SignatureRequest, SignatureResponse}
import org.constellation.schema.transaction.{Transaction, TransactionCacheData}
import org.constellation.schema.{Height, Id, NodeState}
import org.constellation.storage._
import org.constellation.util.Logging.logThread
import org.constellation.util.Metrics

import java.security.KeyPair
import scala.concurrent.ExecutionContext
import scala.util.Random

class CheckpointService[F[_]: Timer: Clock](
  addressService: AddressService[F],
  blacklistedAddresses: BlacklistedAddresses[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  snapshotStorage: SnapshotStorageAlgebra[F],
  checkpointBlockValidator: CheckpointBlockValidator[F],
  nodeStorage: NodeStorageAlgebra[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  rateLimiting: RateLimiting[F],
  dataResolver: DataResolver[F],
  maxWidth: Int,
  maxTipUsage: Int,
  numFacilitatorPeers: Int,
  facilitatorFilter: FacilitatorFilter[F],
  id: Id,
  metrics: Metrics,
  keyPair: KeyPair
)(implicit F: Concurrent[F], C: ContextShift[F]) {

  implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val acceptLock: Semaphore[F] = ConstellationExecutionContext.createSemaphore[F](1)

  import CheckpointService._

  private val snapshotHeightInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  def handleSignatureRequest(sr: SignatureRequest): F[SignatureResponse] =
    checkpointBlockValidator
      .simpleValidation(sr.checkpointBlock)
      .map(_.isValid)
      .map(valid => if (valid) Some(hashSign(sr.checkpointBlock.soeHash, keyPair)) else none[HashSignature])
      .map(SignatureResponse(_))

  def addToAcceptance(checkpoint: FinishedCheckpoint): F[Unit] = addToAcceptance(checkpoint.checkpointCacheData)

  def addToAcceptance(checkpoint: CheckpointCache): F[Unit] = {
    nodeStorage.getNodeState.map(NodeState.canAcceptCheckpoint).ifM(
      for {
        isWaitingForAcceptance <- checkpointStorage.isCheckpointWaitingForAcceptance(checkpoint.checkpointBlock.soeHash)
        isBeingAccepted <- checkpointStorage.isCheckpointInAcceptance(checkpoint.checkpointBlock.soeHash)
        isCheckpointAccepted <- checkpointStorage.isCheckpointAccepted(checkpoint.checkpointBlock.soeHash)
        isWaitingForResolving <- checkpointStorage.isWaitingForResolving(checkpoint.checkpointBlock.soeHash)
        isWaitingForAcceptanceAfterRedownload <- checkpointStorage.isCheckpointWaitingForAcceptanceAfterDownload(checkpoint.checkpointBlock.soeHash)

        _ <- if (isWaitingForAcceptance || isBeingAccepted || isCheckpointAccepted || isWaitingForResolving || isWaitingForAcceptanceAfterRedownload)
          F.unit
        else
          checkpointStorage.persistCheckpoint(checkpoint) >>
            checkpointStorage.registerUsage(checkpoint.checkpointBlock.soeHash) >>
            checkpointStorage.markWaitingForAcceptance(checkpoint.checkpointBlock.soeHash)
      } yield (),
      nodeStorage.getNodeState.map(NodeState.canAwaitForCheckpointAcceptance).ifM(
        checkpointStorage.markForAcceptanceAfterDownload(checkpoint),
        F.unit
      )
    )
  }

  def recalculateQueue(): F[Unit] =
    for {
      blocksForAcceptance <- checkpointStorage.getWaitingForAcceptance
      notAlreadyAccepted <- blocksForAcceptance.toList.filterA { c => checkpointStorage.isCheckpointAccepted(c.checkpointBlock.soeHash).map(!_) }
      lastSnapshotHeight <- snapshotStorage.getLastSnapshotHeight
      aboveLastSnapshotHeight = notAlreadyAccepted.filter { _.height.min > lastSnapshotHeight }

      withNoBlacklistedTxs <- aboveLastSnapshotHeight.filterA { c => AwaitingCheckpointBlock.hasNoBlacklistedTxs(c.checkpointBlock)(blacklistedAddresses) }
      withParentsAccepted <- withNoBlacklistedTxs.filterA(checkpointStorage.areParentsAccepted)
      withReferencesAccepted <- withParentsAccepted.filterA { c => AwaitingCheckpointBlock.areReferencesAccepted(checkpointBlockValidator)(c.checkpointBlock) }

      waitingForResolving <- withReferencesAccepted.toList.filterA { c => checkpointStorage.isWaitingForResolving(c.checkpointBlock.soeHash) }

      alreadyAccepted = blocksForAcceptance.diff(notAlreadyAccepted.toSet)
      _ <- alreadyAccepted.toList.map(_.checkpointBlock.soeHash).traverse { checkpointStorage.unmarkWaitingForAcceptance }

      belowLastSnapshotHeight = notAlreadyAccepted.diff(aboveLastSnapshotHeight)
      _ <- belowLastSnapshotHeight.map(_.checkpointBlock.soeHash).traverse { checkpointStorage.unmarkWaitingForAcceptance }

//      sorted = TopologicalSort.sortBlocksTopologically(withNoBlacklistedTxs).toList
      allowedToAccept = withReferencesAccepted

      withNoParentsAccepted = notAlreadyAccepted.toSet.diff(withParentsAccepted.toSet)
      withNoReferencesAccepted = notAlreadyAccepted.toSet.diff(withReferencesAccepted.toSet)

      _ <- withNoParentsAccepted.toList.map(_.checkpointBlock).traverse { resolveMissingParents }
      _ <- withNoReferencesAccepted.toList.map(_.checkpointBlock).traverse { resolveMissingReferences }

      _ <-
        logger.debug {
          s"All: ${blocksForAcceptance.size} |" +
            s"NotAcc: ${notAlreadyAccepted.size} |" +
            s"ParAcc: ${withParentsAccepted.size} |" +
            s"NoBlack: ${withNoBlacklistedTxs.size} |" +
            s"RefAcc: ${withReferencesAccepted.size} |" +
            s"Resolv: ${waitingForResolving.size} |" +
            s"AlrAcc: ${alreadyAccepted.size} |" +
            s"Below: ${belowLastSnapshotHeight.size}"
        } >>
          metrics.updateMetricAsync("accept_blocksForAcceptance", blocksForAcceptance.size) >>
          metrics.updateMetricAsync("accept_notAlreadyAccepted", notAlreadyAccepted.size) >>
          metrics.updateMetricAsync("accept_withParentsAccepted", withParentsAccepted.size) >>
          metrics.updateMetricAsync("accept_withNoBlacklistedTxs", withNoBlacklistedTxs.size) >>
          metrics.updateMetricAsync("accept_withReferencesAccepted", withReferencesAccepted.size) >>
          metrics.updateMetricAsync("accept_alreadyAccepted", alreadyAccepted.size) >>
          metrics.updateMetricAsync("accept_belowLastSnapshotHeight", belowLastSnapshotHeight.size)

      _ <- checkpointStorage.setAcceptanceQueue(allowedToAccept.map((cbc: CheckpointCache) => cbc.checkpointBlock.soeHash).toSet)
    } yield ()

  def acceptNextCheckpoint(): F[Unit] =
    for {
      allowedToAccept <- checkpointStorage.pullForAcceptance().flatMap { _.fold(none[CheckpointCache].pure[F])(checkpointStorage.getCheckpoint) }

      _ <- allowedToAccept.fold {
        F.unit
      } { cb =>
      acceptLock.withPermit {
        checkpointStorage.markForAcceptance(cb.checkpointBlock.soeHash) >>
          accept(cb)
            .flatMap {
             _ => checkpointStorage.unmarkFromAcceptance(cb.checkpointBlock.soeHash)
            }
            .recoverWith {
              case error => logger.error(error)(s"Checkpoint acceptance error ${cb.checkpointBlock.soeHash}: ${error.getMessage}") >>
                checkpointStorage.unmarkFromAcceptance(cb.checkpointBlock.soeHash)
            }
        }
      }
    } yield ()

  def accept(checkpoint: CheckpointCache): F[Unit] = { // TODO: penalty for facilitators for invalid checkpoint block
    val cb = checkpoint.checkpointBlock
    for {
        _ <- logger.debug(s"[${cb.soeHash}] Acceptance started")

        _ <- logger.debug(s"[${cb.soeHash}] Checking conflicts")
        conflicts <- checkpointBlockValidator.containsAlreadyAcceptedTx(cb)

        _ <- conflicts match {
          case Nil => F.unit
          case xs  =>
//            putConflictingTips(cb.baseHash, cb)
            transactionService
              .removeConflicting(xs)
              .flatMap(_ => F.raiseError[Unit](TipConflictException(cb, conflicts)))
        }

        validation <- checkpointBlockValidator.simpleValidation(cb)
        addressesWithInsufficientBalances = if (validation.isInvalid) getAddressesWithInsufficientBalances(validation)
        else List.empty

        _ <- if (validation.isInvalid)
//          facilitators.toList
//            .traverse(
//              id =>
//                observationService
//                  .put(Observation.create(id, CheckpointBlockInvalid(cb.soeHash, validation.toString))(keyPair))
//            )
//            .flatMap(
//              _ =>
                if (addressesWithInsufficientBalances.nonEmpty)
                  ContainsInvalidTransactionsException(
                    cb,
                    cb.transactions
                      .filter(t => addressesWithInsufficientBalances.contains(t.src.address))
                      .map(_.hash)
                      .toList
                  ).raiseError[F, Unit]
                else F.raiseError[Unit](new Exception(s"CB to accept not valid: $validation"))
//            )
        else F.unit

        height = checkpoint.height

        lastSnapshotHeight <- snapshotStorage.getLastSnapshotHeight
        _ <- if (height.min <= lastSnapshotHeight.toLong) {
          metrics.incrementMetricAsync[F](Metrics.heightBelow) >>
            HeightBelow(checkpoint.checkpointBlock, height).raiseError[F, Unit]
        } else F.unit

        _ <- logger.debug(s"[${cb.soeHash}] Accept data")

        _ <- checkpointStorage.acceptCheckpoint(cb.soeHash)
        doubleSpendTxs <- checkDoubleSpendTransaction(cb)
        _ <- acceptTransactions(cb, Some(checkpoint), doubleSpendTxs.map(_.hash))
        _ <- acceptObservations(cb, Some(checkpoint))
        _ <- updateRateLimiting(cb)
        _ <- transactionService.findAndRemoveInvalidPendingTxs()
        _ <- logger.debug(s"Accept checkpoint=${cb.soeHash}] with height $height")
        _ <- updateTips(cb.soeHash)
//        _ <- snapshotStorage.addAcceptedCheckpointSinceSnapshot(cb.soeHash)
        _ <- metrics.incrementMetricAsync[F](Metrics.checkpointAccepted)
      } yield ()
  }

  def resolveMissingReferences(cb: CheckpointBlock): F[Unit] =
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
              _.filterA(soeHash => checkpointStorage.isWaitingForResolving(soeHash).map(!_))
            }
            .flatMap(_.traverse(hash => dataResolver.resolveCheckpointDefaults(hash)))
          _ <- cbs.traverse(addToAcceptance)
        } yield ()
      } else F.unit
    }

  def resolveMissingParents(cb: CheckpointBlock): F[List[CheckpointCache]] =
    for {
      soeHashes <- cb.parentSOEHashes.toList.pure[F]
      existing <- soeHashes.filterA(checkpointStorage.existsCheckpoint)
      existingButNotWaitingForAcceptance <- existing.filterA(c => checkpointStorage.isCheckpointWaitingForAcceptance(c).map(!_))
      waitingForResolving <- soeHashes.filterA(checkpointStorage.isWaitingForResolving)

      _ <- existingButNotWaitingForAcceptance
        .traverse { checkpointStorage.getCheckpoint }
        .map(_.flatten)
        .flatMap { _.traverse { addToAcceptance } }

      missingSoeHashes = soeHashes.diff(waitingForResolving).diff(existing)
      cbs <- missingSoeHashes.traverse(dataResolver.resolveCheckpointDefaults(_))
      _ <- cbs.traverse(addToAcceptance)
    } yield cbs

  def acceptObservations(cb: CheckpointBlock, cpc: Option[CheckpointCache] = None): F[Unit] =
    cb.observations.toList.traverse(o => observationService.accept(o, cpc)).void

  def acceptTransactions(
    cb: CheckpointBlock,
    cpc: Option[CheckpointCache] = None,
    txsHashToFilter: Seq[String] = Seq.empty
  ): F[Unit] = {
    def toCacheData(tx: Transaction) = TransactionCacheData(
      tx,
      Map(cb.soeHash -> true),
      cbBaseHash = Some(cb.soeHash)
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

  def clearStaleTips(min: Long): F[Unit] =
    (min > snapshotHeightInterval)
      .pure[F]
      .ifM(
        checkpointStorage.getTips.map(_.filter(_._2.min <= min)).flatMap { toRemove =>
          logger
            .debug(
              s"Removing tips that are below cluster height: $min to remove $toRemove"
            )
            .flatMap(_ => checkpointStorage.removeTips(toRemove.map(_._1)))
        },
        logger.debug(
          s"Min=${min} is lower or equal snapshotHeightInterval=${snapshotHeightInterval}. Skipping tips removal"
        )
      )

  def updateTips(soeHash: String): F[Unit] =
    for {
      _ <- checkpointStorage.registerUsage(soeHash)
      parents <- checkpointStorage.getParentSoeHashes(soeHash).map(_.sequence.flatten)
      _ <- logger.debug(s"update_tips | Parents: ${parents}")
      _ <- parents.traverse { parent =>
        for {
          totalSize <- checkpointStorage.countTips
          canReuseTip = totalSize < maxWidth
          areEnoughTipsForConsensus = totalSize > numFacilitatorPeers
          usages <- checkpointStorage.countUsages(soeHash)
          _ <- logger.debug(s"update_tips | Total: ${totalSize} | Usages: ${usages}")
          _ <- if ((usages >= maxTipUsage || !canReuseTip) && areEnoughTipsForConsensus) {
            checkpointStorage.removeTip(parent)
          } else F.unit
        } yield ()
      }
      minTipHeight <- checkpointStorage.getMinTipHeight
      usages <- checkpointStorage.countUsages(soeHash)
      checkpointBlock <- checkpointStorage.getCheckpoint(soeHash)
      totalSize <- checkpointStorage.countTips
      canUseTip = totalSize < maxWidth
      height = checkpointBlock.map(_.height.min).getOrElse(0L)
      _ <- if (height < minTipHeight) {
        logger.warn(s"Block ${soeHash} with height ${height} and usages=${usages} is below the min tip height ${minTipHeight}")
      } else F.unit
      _ <- if (canUseTip && height >= minTipHeight && usages < maxTipUsage) {
        checkpointStorage.addTip(soeHash)
      } else {
        logger.debug(
          s"Block height: ${height} with usages=${usages} above the limit or below min tip: $minTipHeight | update skipped | ($totalSize < $maxWidth && $height >= $minTipHeight && $usages < $maxTipUsage)"
        )
      }

    } yield ()

  def pullTips(readyFacilitators: Map[Id, PeerData])(implicit metrics: Metrics): F[Option[PulledTips]] =
    logThread(
      checkpointStorage.getTips.flatMap { tips =>
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

  private def calculateTipsSOE(tips: Set[(String, Height)]): F[TipSoe] =
    Random
      .shuffle(tips.toSeq.sortBy(_._2.min).take(numFacilitatorPeers)) // TODO: @mwadon
      .take(numFacilitatorPeers)
      .toList
      .traverse {
        case (soeHash, _) =>
          checkpointStorage.getCheckpoint(soeHash)
      }
      .map(_.flatten)
      .map(_.map(cb => (cb.checkpointBlock.soe, cb.height)))
      .map(r => TipSoe(r.map(_._1), r.map(_._2.min).min))

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
          s"[${id.short}] CheckpointBlock with hash=${cb.soeHash} : contains double spend transactions=${doubleSpendTxs
            .map(_.hash)} : from address : ${doubleSpendTxs.map(_.src.address)}"
        ) >> metrics.updateMetricAsync[F]("doubleSpendTransactions", doubleSpendTxs.size)
      else F.unit
      _ <- if (doubleSpendTxs.nonEmpty) blacklistedAddresses.addAll(doubleSpendTxs.map(_.src.address)) else F.unit
    } yield doubleSpendTxs

  private def transfer(tx: Transaction): F[Unit] =
    shouldTransfer(tx).ifM(
      addressService.transferTransaction(tx).void,
      logger.debug(s"[${id.short}] Transaction with hash blocked=${tx.hash} : is dummy=${tx.isDummy}")
    )

  private def shouldTransfer(tx: Transaction): F[Boolean] =
    for {
      isBlacklisted <- isBlacklistedAddress(tx)
      _ <- if (isBlacklisted) metrics.incrementMetricAsync[F]("blockedBlacklistedTxs") else F.unit
      _ <- if (isBlacklisted)
        logger.info(s"[$id.short] Transaction with hash=${tx.hash} : is from blacklisted address=${tx.src.address}")
      else F.unit
      isDummy = tx.isDummy
    } yield !(isBlacklisted || isDummy)

  private def isBlacklistedAddress(tx: Transaction): F[Boolean] =
    blacklistedAddresses.contains(tx.src.address)

  private def updateRateLimiting(cb: CheckpointBlock): F[Unit] =
    rateLimiting.update(cb.transactions.toList)

}

object CheckpointService {

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
