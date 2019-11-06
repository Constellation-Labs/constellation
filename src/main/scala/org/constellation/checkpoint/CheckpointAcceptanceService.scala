package org.constellation.checkpoint

import cats.Applicative
import cats.data.OptionT
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.FinishedCheckpoint
import org.constellation.domain.observation.{
  CheckpointBlockInvalid,
  CheckpointBlockWithMissingParents,
  CheckpointBlockWithMissingSoe,
  Observation,
  ObservationService
}
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.p2p.{Cluster, DataResolver}
import org.constellation.primitives.Schema.{CheckpointCache, Height, NodeState}
import org.constellation.primitives._
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.util.{Metrics, PeerApiClient}
import org.constellation.{ConstellationExecutionContext, DAO}

class CheckpointAcceptanceService[F[_]: Concurrent](
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

  val awaitingForAcceptance: SingleRef[F, Set[CheckpointCache]] = SingleRef(Set())
  val pendingAcceptance: SingleRef[F, Set[String]] = SingleRef(Set())
  val pendingAcceptanceFromOthers: SingleRef[F, Set[String]] = SingleRef(Set())
  val maxDepth: Int = 10

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  // TODO: use combined functions
  def acceptWithReferenceCheck(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] =
    OptionT
      .fromOption[F](checkpoint.checkpointCacheData.checkpointBlock)
      .semiflatMap(isCheckpointBlockAllowedForAcceptance[F](_)(transactionService.transactionChainService))
      .value
      .flatMap {
        case Some(true)  => accept(checkpoint)
        case Some(false) => awaitingForAcceptance.unsafeModify(s => (s + checkpoint.checkpointCacheData, ()))
        case _           => Sync[F].raiseError[Unit](MissingCheckpointBlockException)
      }

  def acceptWithNodeCheck(checkpoint: FinishedCheckpoint)(implicit cs: ContextShift[F]): F[Unit] =
    cluster.getNodeState.flatMap { nodeState =>
      (nodeState, checkpoint.checkpointCacheData.checkpointBlock) match {
        case (NodeState.Ready, Some(_)) => acceptWithReferenceCheck(checkpoint)
        case (NodeState.DownloadCompleteAwaitingFinalSync, Some(_)) =>
          snapshotService.syncBufferAccept(checkpoint)
        case (_, Some(_)) => Sync[F].raiseError[Unit](PendingDownloadException(dao.id))
        case (_, None)    => Sync[F].raiseError[Unit](MissingCheckpointBlockException)
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
          _ <- logger.debug(s"[${dao.id.short}] starting accept block: ${cb.baseHash} from others")
          peers <- obtainPeers
          _ <- resolveMissingParents(cb, peers)
          _ <- accept(checkpoint.checkpointCacheData, checkpoint.facilitators)
          _ <- pendingAcceptanceFromOthers.update(_.filterNot(_ == cb.baseHash))
        } yield ()

        acceptance.recoverWith {
          case ex: PendingAcceptance =>
            acceptErrorHandler(ex)
          case error =>
            pendingAcceptanceFromOthers.update(_.filterNot(_ == cb.baseHash)) >> acceptErrorHandler(error)
        }

    }
  }

  def resolveMissingParents(
    cb: CheckpointBlock,
    peers: List[PeerApiClient],
    depth: Int = 1
  ): F[List[CheckpointCache]] = {

    val checkError = if (depth >= maxDepth) {
      Sync[F].raiseError[Unit](new Exception("Max depth reached when resolving data."))
    } else Sync[F].unit

    val resolveSoe = checkpointParentService
      .parentSOEBaseHashes(cb)
      .flatMap({
        case List(_, _) => Sync[F].unit
        case _ =>
          LiftIO[F]
            .liftIO(DataResolver.resolveSoe(cb.parentSOEHashes.toList, peers)(contextShift)(dao = dao).void)
            .flatTap(
              _ =>
                peers.traverse(
                  p =>
                    observationService
                      .put(Observation.create(p.id, CheckpointBlockWithMissingSoe(cb.baseHash))(dao.keyPair))
                )
            )
      })

    val resolveCheckpoint =
      checkpointParentService
        .parentSOEBaseHashes(cb)
        .map(
          parents => parents.traverse(h => checkpointService.contains(h).map(exist => (h, exist)))
        )
        .flatten
        .flatMap {
          case Nil =>
            Sync[F]
              .raiseError[List[CheckpointCache]](new RuntimeException("Soe hashes are empty even resolved previously"))
          case List((_, true), (_, true)) => Sync[F].pure(List[CheckpointCache]())
          case missing =>
            LiftIO[F]
              .liftIO(DataResolver.resolveCheckpoints(missing.map(_._1), peers)(contextShift)(dao = dao))
              .flatTap(
                _ =>
                  peers.traverse(
                    p =>
                      observationService
                        .put(Observation.create(p.id, CheckpointBlockWithMissingParents(cb.baseHash))(dao.keyPair))
                  )
              )
        }

    for {
      _ <- checkError
      _ <- resolveSoe
      resolved <- resolveCheckpoint
      all <- resolved.traverse(c => resolveMissingParents(c.checkpointBlock.get, peers, depth + 1))
    } yield all.flatten

  }

  def accept(checkpoint: CheckpointCache, facilitators: Set[Id] = Set.empty)(implicit cs: ContextShift[F]): F[Unit] = {

    val acceptCheckpoint: F[Unit] = checkpoint.checkpointBlock match {
      case None => Sync[F].raiseError[Unit](MissingCheckpointBlockException)

      case Some(cb) =>
        for {
          _ <- syncPending(pendingAcceptance, cb.baseHash)
          _ <- checkpointService
            .contains(cb.baseHash)
            .ifM(
              dao.metrics
                .incrementMetricAsync[F]("checkpointAcceptBlockAlreadyStored") >> CheckpointAcceptBlockAlreadyStored(cb)
                .raiseError[F, Unit],
              Sync[F].unit
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
          _ <- LiftIO[F].liftIO(cb.storeSOE()(dao))
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
          _ <- pendingAcceptance.update(_.filterNot(_ == cb.baseHash))
          awaiting <- awaitingForAcceptance.modify { s =>
            val ret = s.filterNot(_.checkpointBlock.map(_.baseHash).getOrElse("unknown") == cb.baseHash)
            (ret, ret)
          }
          allowedToAccept <- awaiting
            .flatMap(cc => cc.checkpointBlock.map((_, cc)))
            .toList
            .filterA(c => isCheckpointBlockAllowedForAcceptance[F](c._1)(transactionService.transactionChainService))
            .map(_.map(_._2))
          _ <- logger.info(s"Awaiting: ${awaiting.size} | Allowed to accept: ${allowedToAccept.size}")
          _ <- cs.shift >> allowedToAccept.traverse(accept(_))
        } yield ()
    }

    acceptCheckpoint.recoverWith {
      case ex @ (PendingAcceptance(_) | MissingCheckpointBlockException) =>
        acceptErrorHandler(ex)
      case error =>
        pendingAcceptance.update(_.filterNot(_ == checkpoint.checkpointBlock.get.baseHash)) >> acceptErrorHandler(error)
    }
  }

  private[checkpoint] def syncPending(storage: SingleRef[F, Set[String]], baseHash: String): F[Unit] =
    storage.update { hashes =>
      if (hashes.contains(baseHash)) {
        throw PendingAcceptance(baseHash)
      } else {
        hashes + baseHash
      }
    }

  def acceptErrorHandler(err: Throwable): F[Unit] =
    err match {
      case knownError @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
        knownError.raiseError[F, Unit]
      case otherError =>
        Sync[F].delay(logger.error(s"Error when accepting block: ${otherError.getMessage}")) >> dao.metrics
          .incrementMetricAsync[F]("acceptCheckpoint_failure") >> otherError.raiseError[F, Unit]
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
          if (m.signedMessageData.data.previousMessageHash != Genesis.CoinBaseHash) {
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
    cb.transactions.toList
      .groupBy(_.src.address)
      .mapValues(_.sortBy(_.lastTxRef.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            txChainService
              .getLastAcceptedTransactionRef(hash)
              .map(_.ordinal == txs.headOption.map(_.lastTxRef.ordinal).getOrElse(-1))
              .ifM(
                true.pure[F],
                false.pure[F]
              )
        }
      }
      .map(_.forall(_ == true))
}
