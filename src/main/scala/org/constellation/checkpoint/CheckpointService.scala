package org.constellation.checkpoint

import java.security.KeyPair

import cats.effect.concurrent.Semaphore
import cats.effect.{Clock, Concurrent, ContextShift, Timer}
import cats.syntax.all._
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation._
import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.consensus.FacilitatorFilter
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.checkpointBlock.{
  AwaitingCheckpointBlock,
  CheckpointBlockDoubleSpendChecker,
  CheckpointStorageAlgebra
}
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.p2p.DataResolver.DataResolverCheckpointsEnqueue
import org.constellation.p2p.{DataResolver, PeerData}
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, FinishedCheckpoint}
import org.constellation.schema.signature.{HashSignature, SignatureRequest, SignatureResponse}
import org.constellation.schema.transaction.{Transaction, TransactionCacheData}
import org.constellation.schema.{Height, Id, NodeState}
import org.constellation.storage._
import org.constellation.util.Logging.logThread
import org.constellation.util.Metrics

import scala.util.Random

class CheckpointService[F[_]: Timer: Clock](
  addressService: AddressService[F],
  blacklistedAddresses: BlacklistedAddresses[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  snapshotStorage: SnapshotStorageAlgebra[F],
  checkpointBlockValidator: CheckpointBlockValidator[F],
  nodeStorage: NodeStorageAlgebra[F],
  clusterStorage: ClusterStorageAlgebra[F],
  apiClient: ClientInterpreter[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  rateLimiting: RateLimiting[F],
  dataResolver: DataResolver[F],
  maxWidth: Int,
  maxTipUsage: Int,
  numFacilitatorPeers: Int,
  facilitatorFilter: FacilitatorFilter[F],
  id: Id,
  metrics: Metrics,
  keyPair: KeyPair,
  checkpointsQueueInstance: DataResolverCheckpointsEnqueue[F]
)(implicit F: Concurrent[F], C: ContextShift[F]) {

  implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val acceptLock: Semaphore[F] = ConstellationExecutionContext.createSemaphore[F](1)

  import CheckpointService._

  private val toExclude: Set[String] = Set(
    "c0e4b08a9cb7cd0fd8237aa2bc7fce5998d0153572c365fa1166d3511277c974",
    "5594062fcba0a3798e274471e28e5dc0d2e8fd7c69c0a6869b8707e00489d0af",
    "afa09526e0f876e341a6a504e5dbd1bb27f2b36aecde7127e5dc49c4c0e70659",
    "8e68e65ab3c1e6e8b92772af6c34ef8f82dc078af9a5ce8c46971c39cd35d98d",
    "3b39e6be4f43b9f3380f23a64ff11d99c330f1cec02e70255dd423d3571903ef",
    "f77adbc2cef5a26d86c3161332a86433ed282c9c9de6596ea0444d4e7aaaa978",
    "ac071f37093ff1257697a250ef91318494a9383cd6fc0741d5887aa28273bc8d",
    "4a563f2eb425755854ea3d53873f6097f35c2bc589ab6d76725a3dcdfe21e5ac",
    "c541c0d2ccf5d7a1062f367a18f309234dd60d2e22ee33c3c56e0fa4dc4aea51",
    "228be87b6ca7f5dc5fbd014a902efb92d6946f0a035a719fbeb9de9cac8f9609",
    "f254a9853c5428a73ccddace9fe9920bb507442ea532ee2421bf461009389676",
    "f630edaf1f745b72878dc66d076da402ed5ea52b065cbb50427cb0c95186d875",
    "515061c326ac2357e216c6a4f94c708e4f2f6a082463e6d3712c7022bd8ce8b8",
    "7bca0c2ae4ba0720cdc813aed0f56e779a905432b115609ba0997f701951d426",
    "0f4c6fd20f8b77dd67aa3db39675e76b44167563c3c487da49659c3781974b8f",
    "ac071f37093ff1257697a250ef91318494a9383cd6fc0741d5887aa28273bc8d",
    "de95b5a15a581cc73c5037162a7ae4c3d1457537670ef5c830557a669b305e61",
    "d0b989761216a2150c77d614cfd39d9ccc64aca7bee92ccd7a992777072dc995",
    "3043a186541c97d44a7a66e8bf1420fcc585e99fe622c248c45c803df8699c18",
    "577bd2e1be94c58f4f40d40bd917d50a3697e312a3a9ea40c342d8a41ca21f03",
    "88db83e2b1042e08b720a51e206553512282ba04bc0f99a7bfde8ff0d01965fd",
    "ad18ea6439c065c43d581684bb354d880a53afb0135980c314dc5d6b8f6cf496",
    "047d045e1f275597934206b8a9a0552cb31b05ea8630e74e40ef505e953884aa",
    "e8bece52e0af864cd7f9c031a1d9acf8131b8c7ebeceb6687186781adc3b1dbb",
    "a2550d64b8f265d7f338a3f38a7484c118e2c3844a93b05b46dcd64685ca7417",
    "2420e2609c5a0efd9b8104dc2346fd4da37f75971650de67cd69b19f85a09e11",
    "6e716510abb4123d39e2c860d70baac67837487b731f16b23c3e5ddc23ea939e",
    "e4629bc2239b29c3f23b9767b63c10a35db89e8399533c56097cad1188a3ab26"
  )

  private val snapshotHeightInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  def handleSignatureRequest(sr: SignatureRequest): F[SignatureResponse] =
    checkpointBlockValidator
      .simpleValidation(sr.checkpointBlock)
      .map(_.isValid)
      .map(valid => if (valid) Some(hashSign(sr.checkpointBlock.soeHash, keyPair)) else none[HashSignature])
      .map(SignatureResponse(_))

  def addToAcceptance(checkpoint: FinishedCheckpoint): F[Unit] = addToAcceptance(checkpoint.checkpointCacheData)

  def addToAcceptance(checkpoint: CheckpointCache): F[Unit] =
    nodeStorage.getNodeState
      .map(NodeState.canAcceptCheckpoint)
      .ifM(
        for {
          isValid <- checkpointBlockValidator.simpleValidation(checkpoint.checkpointBlock).map(_.isValid)

          isWaitingForAcceptance <- checkpointStorage
            .isCheckpointWaitingForAcceptance(checkpoint.checkpointBlock.soeHash)
          isBeingAccepted <- checkpointStorage.isCheckpointInAcceptance(checkpoint.checkpointBlock.soeHash)
          isCheckpointAccepted <- checkpointStorage.isCheckpointAccepted(checkpoint.checkpointBlock.soeHash)
          isWaitingForResolving <- checkpointStorage.isWaitingForResolving(checkpoint.checkpointBlock.soeHash)
          isWaitingForAcceptanceAfterRedownload <- checkpointStorage
            .isCheckpointWaitingForAcceptanceAfterDownload(checkpoint.checkpointBlock.soeHash)

          _ <- if (!isValid || isWaitingForAcceptance || isBeingAccepted || isCheckpointAccepted || isWaitingForResolving || isWaitingForAcceptanceAfterRedownload)
            F.unit
          else
            checkpointStorage.persistCheckpoint(checkpoint) >>
              checkpointStorage.registerUsage(checkpoint.checkpointBlock.soeHash) >>
              checkpointStorage.markWaitingForAcceptance(checkpoint.checkpointBlock.soeHash)
        } yield (),
        nodeStorage.getNodeState
          .map(NodeState.canAwaitForCheckpointAcceptance)
          .ifM(
            checkpointStorage.markForAcceptanceAfterDownload(checkpoint),
            F.unit
          )
      )

  def recalculateQueue(): F[Unit] =
    for {
      blocksForAcceptance <- checkpointStorage.getWaitingForAcceptance

      valid <- blocksForAcceptance.toList.filterA { c =>
        checkpointBlockValidator.simpleValidation(c.checkpointBlock).map(_.isValid)
      }.map(_.toSet)

      accepted <- checkpointStorage.getAccepted
      inSnapshot <- checkpointStorage.getInSnapshot.map(_.map(_._1))
      acceptedPool = accepted ++ inSnapshot

      notAlreadyAccepted = valid.filterNot(cb => acceptedPool.contains(cb.checkpointBlock.soeHash)).toList

      lastSnapshotHeight <- snapshotStorage.getLastSnapshotHeight
      aboveLastSnapshotHeight = notAlreadyAccepted.filter { _.height.min > lastSnapshotHeight }

      withNoBlacklistedTxs <- aboveLastSnapshotHeight.filterA { c =>
        AwaitingCheckpointBlock.hasNoBlacklistedTxs(c.checkpointBlock)(blacklistedAddresses)
      }

      withParentsAccepted = withNoBlacklistedTxs.filter(
        soeHash => checkpointStorage.areParentsAccepted(soeHash, acceptedPool.contains)
      )

      withReferencesAccepted <- withParentsAccepted.filterA { c =>
        AwaitingCheckpointBlock.areReferencesAccepted(checkpointBlockValidator)(c.checkpointBlock)
      }

//      sorted = TopologicalSort.sortBlocksTopologically(withNoBlacklistedTxs).toList
      allowedToAccept = withReferencesAccepted

      _ <- checkpointStorage.setAcceptanceQueue(
        allowedToAccept.map((cbc: CheckpointCache) => cbc.checkpointBlock.soeHash).toSet
      )

      invalid = blocksForAcceptance.diff(valid)
      _ <- invalid.toList.map(_.checkpointBlock.soeHash).traverse {
        checkpointStorage.unmarkWaitingForAcceptance
      }

      alreadyAccepted = valid.diff(notAlreadyAccepted.toSet)
      _ <- alreadyAccepted.toList.map(_.checkpointBlock.soeHash).traverse {
        checkpointStorage.unmarkWaitingForAcceptance
      }

      belowLastSnapshotHeight = notAlreadyAccepted.diff(aboveLastSnapshotHeight)
      _ <- belowLastSnapshotHeight.map(_.checkpointBlock.soeHash).traverse {
        checkpointStorage.unmarkWaitingForAcceptance
      }

      withNoParentsAccepted = notAlreadyAccepted.toSet.diff(withParentsAccepted.toSet)
      _ <- F.start {
        withNoParentsAccepted.toList.map(_.checkpointBlock).traverse { resolveMissingParents }
      }

      withNoReferencesAccepted = notAlreadyAccepted.toSet.diff(withReferencesAccepted.toSet)
      _ <- F.start {
        withNoReferencesAccepted.toList.map(_.checkpointBlock).traverse { resolveMissingReferences }
      }

      waitingForResolving <- withReferencesAccepted.filterA { c =>
        checkpointStorage.isWaitingForResolving(c.checkpointBlock.soeHash)
      }

      blocksWithNoParents = withNoParentsAccepted.map(_.checkpointBlock.soeHash).intersect(toExclude)
      blocksWithNoReferences = withNoReferencesAccepted.map(_.checkpointBlock.soeHash).intersect(toExclude)

      _ <- if (blocksWithNoParents.nonEmpty) logger.debug {
        s"[acc] Removing blocks with no parents: ${blocksWithNoParents}"
      } else F.unit
      _ <- if (blocksWithNoReferences.nonEmpty) logger.debug {
        s"[acc] Removing blocks with no references: ${blocksWithNoReferences}"
      } else F.unit

      _ <- blocksWithNoParents.toList.traverse { checkpointStorage.removeCheckpoint }
      _ <- blocksWithNoReferences.toList.traverse { checkpointStorage.removeCheckpoint }

      _ <- logger.debug { s"[acc] Already accepted: ${alreadyAccepted.map(_.checkpointBlock.soeHash)}" }
      _ <- logger.debug {
        s"[acc] Below last snapshot height: ${belowLastSnapshotHeight.map(_.checkpointBlock.soeHash)}"
      }

      _ <- logger.debug {
        s"All: ${blocksForAcceptance.size} |" +
          s"Val: ${valid.size} | " +
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
        metrics.updateMetricAsync("accept_belowLastSnapshotHeight", belowLastSnapshotHeight.size) >>
        metrics.updateMetricAsync("accept_invalid", invalid.size)
    } yield ()

  def acceptNextCheckpoint(): F[Unit] =
    for {
      allowedToAccept <- checkpointStorage.pullForAcceptance().flatMap {
        _.fold(none[CheckpointCache].pure[F])(checkpointStorage.getCheckpoint)
      }

      _ <- allowedToAccept.fold {
        F.unit
      } { cb =>
        acceptLock.withPermit {
          checkpointStorage.markForAcceptance(cb.checkpointBlock.soeHash) >>
            accept(cb).flatMap { _ =>
              checkpointStorage.unmarkFromAcceptance(cb.checkpointBlock.soeHash)
            }.recoverWith {
              case error =>
                logger.error(error)(s"Checkpoint acceptance error ${cb.checkpointBlock.soeHash}: ${error.getMessage}") >>
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
          _ <- allTxs
            .flatMap(_.cbBaseHash)
            .distinct
            .filterA(checkpointStorage.existsCheckpoint(_).map(!_))
            .flatMap {
              _.filterA(soeHash => checkpointStorage.isWaitingForResolving(soeHash).map(!_))
            }
            .flatMap { _.traverse { checkpointsQueueInstance.enqueueCheckpoint(_, None, addToAcceptance) } }
        } yield ()
      } else F.unit
    }

  def resolveMissingParents(cb: CheckpointBlock): F[Unit] =
    for {
      soeHashes <- cb.parentSOEHashes.toList.pure[F]
      existing <- soeHashes.filterA(checkpointStorage.existsCheckpoint)
      existingButNotWaitingForAcceptance <- existing.filterA(
        c => checkpointStorage.isCheckpointWaitingForAcceptance(c).map(!_)
      )
      waitingForResolving <- soeHashes.filterA(checkpointStorage.isWaitingForResolving)

      _ <- existingButNotWaitingForAcceptance.traverse { checkpointStorage.getCheckpoint }
        .map(_.flatten)
        .flatMap { _.traverse { addToAcceptance } }

      missingSoeHashes = soeHashes.diff(waitingForResolving).diff(existing)
      _ <- missingSoeHashes.traverse { checkpointsQueueInstance.enqueueCheckpoint(_, None, addToAcceptance) }
    } yield ()

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
            .flatTap { _ =>
              toRemove.toList.traverse { _ =>
                metrics.incrementMetricAsync[F]("tips_staleRemoved")
              }
            }
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
      _ <- if (height <= minTipHeight) {
        logger.warn(
          s"Block ${soeHash} with height ${height} and usages=${usages} is below or equal the min tip height ${minTipHeight}"
        )
      } else F.unit
      _ <- if (canUseTip && height > minTipHeight && usages < maxTipUsage) {
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
            calculateTipsSOE(tips).flatMap {
              tipSOE =>
                facilitatorFilter.filterPeers(facilitators, numFacilitatorPeers, tipSOE).map {
                  case f if f.size >= numFacilitatorPeers =>
                    // TODO: joining pool note: calculateFinalFacilitators seems not needed as filterPeers will return the number of facilitators equal (or smaller) to numFacilitatorPeers
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

  def compareAcceptedCheckpoints(): F[Unit] =
    for {
      peers <- clusterStorage.getActivePeers
      randomPeerSeq <- F.delay { Random.shuffle(peers.values.toSeq).take(1) }
      randomPeer <- if (randomPeerSeq.nonEmpty) randomPeerSeq.head.pure[F]
      else F.raiseError[PeerData](new Throwable(s"No peers to compare accepted checkpoints."))
      peerClientMetadata = randomPeer.peerMetadata.toPeerClientMetadata

      joinedHeightOpt <- nodeStorage.getOwnJoinedHeight
      _ <- if (joinedHeightOpt.nonEmpty) F.unit else F.raiseError[Unit](new Throwable("Not connected yet"))

      joinedHeight = joinedHeightOpt.get

      lastSnapshotHeight <- snapshotStorage.getLastSnapshotHeight
      acceptedAtHeights <- apiClient.checkpoint
        .getAcceptedHash()
        .run(peerClientMetadata)
        .map(_.filterKeys(_ > joinedHeight))
        .map(_.filterKeys(_ > lastSnapshotHeight))
      ownAcceptedAtHeights <- checkpointStorage.getAcceptedHash
        .map(_.filterKeys(_ > joinedHeight))
        .map(_.filterKeys(_ > lastSnapshotHeight))
      diff = acceptedAtHeights.filterNot {
        case (height, hash) =>
          ownAcceptedAtHeights.exists { case (height2, hash2) => height == height2 && hash == hash2 }
      }
      diffHeights = diff.keySet.toList

      _ <- if (diffHeights.isEmpty) logger.debug(s"Compare accepted blocks with ${peerClientMetadata.id.short}: equal")
      else
        for {
          _ <- logger.debug(
            s"Compare accepted blocks with ${peerClientMetadata.id.short}: difference at heights ${diffHeights}"
          )

          hashesAtHeight <- diffHeights.traverse { height =>
            apiClient.checkpoint.getAcceptedAtHeight(height).run(peerClientMetadata).map(s => (height, s))
          }.map(_.toMap)
          ownAtHeight <- diffHeights.traverse { height =>
            checkpointStorage.getAcceptedAtHeight(height).map(s => (height, s))
          }.map(_.toMap)
          diffHashes = ownAtHeight.flatMap {
            case (height, hashes) => hashesAtHeight.getOrElse(height, List.empty).diff(hashes)
          }.toList

          missingHashes <- diffHashes
            .filterA(soeHash => checkpointStorage.getCheckpoint(soeHash).map(_.isEmpty))
            .flatMap(_.filterA(soeHash => checkpointStorage.isCheckpointWaitingForAcceptance(soeHash).map(!_)))
            .flatMap(_.filterA(soeHash => checkpointStorage.isCheckpointAccepted(soeHash).map(!_)))
            .flatMap(_.filterA(soeHash => checkpointStorage.isWaitingForResolving(soeHash).map(!_)))
            .flatMap(_.filterA(soeHash => checkpointStorage.isCheckpointAwaiting(soeHash).map(!_)))
            .flatMap(
              _.filterA(soeHash => checkpointStorage.isCheckpointWaitingForAcceptanceAfterDownload(soeHash).map(!_))
            )

          _ <- logger.debug(
            s"Compare accepted blocks with ${peerClientMetadata.id.short}: missing hashes ${missingHashes}"
          )
          _ <- missingHashes.traverse { _ =>
            metrics.incrementMetricAsync[F]("compare_missingBlock")
          }

          _ <- missingHashes.traverse {
            checkpointsQueueInstance.enqueueCheckpoint(_, Some(peerClientMetadata), addToAcceptance)
          }
        } yield ()
    } yield ()

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
