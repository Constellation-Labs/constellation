package org.constellation.checkpoint

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.FinishedCheckpoint
import org.constellation.domain.checkpointBlock.AwaitingCheckpointBlock
import org.constellation.domain.observation.{
  CheckpointBlockInvalid,
  CheckpointBlockWithMissingParents,
  CheckpointBlockWithMissingSoe,
  Observation,
  ObservationService
}
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.p2p.{Cluster, DataResolver}
import org.constellation.primitives.Schema.{CheckpointCache, Height, NodeState, SignedObservationEdgeCache}
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.util.{Metrics, PeerApiClient}
import org.constellation.{ConstellationExecutionContext, DAO}

import scala.concurrent.duration._

class CheckpointAcceptanceService[F[_]: Concurrent: Timer](
  addressService: AddressService[F],
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

  val acceptLock: Semaphore[F] = ConstellationExecutionContext.createSemaphore[F](1)

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

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
    cluster.getNodeState.flatMap { nodeState =>
      (nodeState, checkpoint.checkpointCacheData.checkpointBlock) match {
        case (_, None)                                          => Sync[F].raiseError[Unit](MissingCheckpointBlockException)
        case (state, _) if NodeState.canAcceptCheckpoint(state) => accept(checkpoint)
        case (state, _) if NodeState.canAwaitForCheckpointAcceptance(state) =>
          snapshotService.syncBufferAccept(checkpoint)
        case (_, _) => Sync[F].raiseError[Unit](PendingDownloadException(dao.id))
      }
    }

  def accept(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] = {
    val obtainPeers = cluster.getPeerInfo.map { allPeers =>
      val filtered = allPeers.filter(t => checkpoint.facilitators.contains(t._1))
      (if (filtered.isEmpty) allPeers else filtered)
        .map(p => PeerApiClient(p._1, p._2.client))
        .toList
    }

    checkpoint.checkpointCacheData.checkpointBlock match {
      case None => Sync[F].raiseError[Unit](MissingCheckpointBlockException)
      case Some(cb) =>
        val acceptance = for {
          _ <- syncPending(pendingAcceptanceFromOthers, cb.baseHash)
          _ <- checkPending(cb.baseHash)
          _ <- logger.debug(s"[${dao.id.short}] starting accept block: ${cb.baseHash} from others")
          peers <- obtainPeers
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
  }

  def accept(checkpoint: CheckpointCache, facilitators: Set[Id] = Set.empty)(implicit cs: ContextShift[F]): F[Unit] = {

    val acceptCheckpoint: F[Unit] = checkpoint.checkpointBlock match {
      case None => Sync[F].raiseError[Unit](MissingCheckpointBlockException)

      case Some(cb) =>
        for {
          _ <- acceptLock.acquire
          _ <- syncPending(pendingAcceptance, cb.baseHash)
          _ <- checkpointService
            .contains(cb.baseHash)
            .ifM(
              dao.metrics
                .incrementMetricAsync[F]("checkpointAcceptBlockAlreadyStored") >> CheckpointAcceptBlockAlreadyStored(cb)
                .raiseError[F, Unit],
              Sync[F].unit
            )

          _ <- AwaitingCheckpointBlock
            .areParentsSOEAccepted(checkpointParentService.soeService)(cb)
            .ifM(
              Sync[F].unit,
              awaiting.modify(s => (s + checkpoint, ())) >> MissingParents(cb).raiseError[F, Unit]
            )

          _ <- AwaitingCheckpointBlock
            .areReferencesAccepted(transactionService.transactionChainService)(cb)
            .ifM(
              logConditions(cb, true) >> Sync[F].unit,
              logConditions(cb, false) >> awaiting.modify(
                s => (s + checkpoint, ())
              ) >> MissingTransactionReference(cb).raiseError[F, Unit]
            )

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
          _ <- if (!validation.isValid)
            facilitators.toList
              .traverse(
                id =>
                  observationService
                    .put(Observation.create(id, CheckpointBlockInvalid(cb.baseHash, validation))(dao.keyPair))
              )
              .flatMap(_ => Sync[F].raiseError[Unit](new Exception(s"CB to accept not valid: $validation")))
          else Sync[F].unit
          _ <- checkpointParentService.soeService
            .put(cb.soeHash, SignedObservationEdgeCache(cb.soe, true)) // TODO: consider moving down
          maybeHeight <- checkpointParentService.calculateHeight(cb).map(h => if (h.isEmpty) checkpoint.height else h)

          height <- if (maybeHeight.isEmpty) {
            dao.metrics
              .incrementMetricAsync[F](Metrics.heightEmpty)
              .flatMap(_ => MissingHeightException(cb).raiseError[F, Height])
          } else Sync[F].pure(maybeHeight.get)

          _ <- checkpointService.put(checkpoint.copy(height = maybeHeight))
          _ <- checkpointParentService.incrementChildrenCount(cb)
          _ <- Sync[F].delay(dao.recentBlockTracker.put(checkpoint.copy(height = maybeHeight)))
          _ <- acceptMessages(cb)
          _ <- acceptTransactions(cb, Some(checkpoint))
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
          _ <- acceptLock.release
          awaitingBlocks <- awaiting.modify { s =>
            val ret = s.filterNot(_.checkpointBlock.map(_.baseHash).getOrElse("unknown") == cb.baseHash)
            (ret, ret)
          }
          allowedToAccept <- awaitingBlocks.toList.filterA { c =>
            c.checkpointBlock
              .map(AwaitingCheckpointBlock.areReferencesAccepted(transactionService.transactionChainService))
              .getOrElse(false.pure[F])
          }.flatMap(_.filterA { c =>
            c.checkpointBlock
              .map(AwaitingCheckpointBlock.areParentsSOEAccepted(checkpointParentService.soeService))
              .getOrElse(false.pure[F])
          })

          _ <- dao.metrics.updateMetricAsync[F]("awaitingForAcceptance", awaitingBlocks.size)
          _ <- dao.metrics.updateMetricAsync[F]("allowedForAcceptance", allowedToAccept.size)
          _ <- logger.debug(
            s"Awaiting for acceptance: ${awaitingBlocks.size} | Allowed to accept: ${allowedToAccept.size}"
          )

          _ <- Concurrent[F].start(allowedToAccept.traverse(accept(_)))
        } yield ()
    }

    acceptCheckpoint.recoverWith {
      case ex @ (PendingAcceptance(_) | MissingCheckpointBlockException) =>
        acceptErrorHandler(ex)
      case error =>
        pendingAcceptance.modify(pa => (pa.filterNot(_ == checkpoint.checkpointBlock.get.baseHash), ())) >> acceptErrorHandler(
          error
        )
    }
  }

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
      if (cbs.map(_.checkpointBlock.map(_.baseHash).getOrElse("unknown")).contains(baseHash)) {
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
          t <- LiftIO[F].liftIO(DataResolver.resolveBatchTransactionsDefaults(txs)(contextShift))
          _ <- logger.info(s"${Console.YELLOW}${t.map(tx => (tx.hash, tx.cbBaseHash))}${Console.RESET}")
          cbs <- t.flatMap(_.cbBaseHash).distinct.traverse { hash =>
            LiftIO[F].liftIO(DataResolver.resolveCheckpointDefaults(hash)(contextShift))
          }
          _ <- cbs.traverse(accept(_))
        } yield ()
      } else Sync[F].unit
    }
  }

  def resolveMissingParents(
    cb: CheckpointBlock,
    peers: List[PeerApiClient],
    depth: Int = 1
  ): F[List[CheckpointCache]] = {

    val soeHashes = cb.parentSOEHashes.toList
    val existingSoeHashes = soeHashes
      .traverse(checkpointParentService.soeService.lookup)
      .map(_.flatten)
      .map(_.map(_.signedObservationEdge.hash))
    val missingSoeHashes = existingSoeHashes.map(existing => soeHashes.diff(existing))

    val resolveCheckpoints = missingSoeHashes
      .map(_.flatMap(_ => checkpointParentService.parentBaseHashesDirect(cb)))
      .flatMap(_.traverse(hash => checkpointService.contains(hash).map(exist => (hash, exist))))
      .map(_.filter(_._2))
      .map(_.map(_._1))
      .flatMap {
        _.traverse { hash =>
          LiftIO[F].liftIO(DataResolver.resolveCheckpointDefaults(hash)(contextShift)(dao = dao))
        }
      }

    // TODO: Add observation

    resolveCheckpoints
  }

  def acceptErrorHandler(err: Throwable)(implicit cs: ContextShift[F]): F[Unit] =
    err match {
      case knownError @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
        acceptLock.release >>
          knownError.raiseError[F, Unit]

      case error @ MissingTransactionReference(cb) =>
        acceptLock.release >>
          Concurrent[F].start(Timer[F].sleep(10.seconds) >> resolveMissingReferences(cb)) >>
          error.raiseError[F, Unit]
      case otherError =>
        acceptLock.release >>
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

  def acceptTransactions(cb: CheckpointBlock, cpc: Option[CheckpointCache] = None): F[Unit] = {
    def toCacheData(tx: Transaction) = TransactionCacheData(
      tx,
      Map(cb.baseHash -> true),
      cbBaseHash = Some(cb.baseHash)
    )

    val insertTX =
      LiftIO[F].liftIO {
        cb.transactions.toList
          .map(tx ⇒ (tx, toCacheData(tx)))
          .traverse {
            case (tx, txMetadata) =>
              dao.transactionService.accept(txMetadata, cpc) >> transferIfNotDummy(tx)
          }
          .void
      }

    insertTX
  }

  private def transferIfNotDummy(transaction: Transaction): IO[Unit] =
    if (!transaction.isDummy) dao.addressService.transfer(transaction).void else IO.unit

  private def updateRateLimiting(cb: CheckpointBlock): F[Unit] =
    rateLimiting.update(cb.transactions.toList)

  private def acceptMessages(cb: CheckpointBlock): F[List[Unit]] =
    LiftIO[F].liftIO {
      cb.messages.map { m =>
        val channelMessageMetadata = ChannelMessageMetadata(m, Some(cb.baseHash))
        val messageUpdate =
          if (m.signedMessageData.data.previousMessageHash != Genesis.Coinbase) {
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
          } else { // Unsafe json extract
            dao.channelService.put(
              m.signedMessageData.hash,
              ChannelMetadata(
                m.signedMessageData.data.message.x[ChannelOpen],
                channelMessageMetadata
              )
            )
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
                } else txs.map(_.lastTxRef.hash).filterNot(_ == "")
              })
        }
      }
}
