package org.constellation.storage

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.consensus.FinishedCheckpoint
import org.constellation.p2p.{DataResolver, PeerNotification}
import org.constellation.primitives.Schema.{CheckpointCache, _}
import org.constellation.primitives._
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.util.{Metrics, PeerApiClient}

class CheckpointService[F[_]: Concurrent](
  dao: DAO,
  transactionService: TransactionService[F],
  messageService: MessageService[F],
  notificationService: NotificationService[F],
  observationService: ObservationService[F],
  concurrentTipService: ConcurrentTipService[F],
  rateLimiting: RateLimiting[F]
) extends StrictLogging {

  val memPool = new CheckpointBlocksMemPool[F](
    dao,
    transactionService.merklePool,
    messageService.merklePool,
    notificationService.merklePool,
    observationService.merklePool
  )
  val pendingAcceptance: SingleRef[F, Set[String]] = SingleRef(Set())
  val pendingAcceptanceFromOthers: SingleRef[F, Set[String]] = SingleRef(Set())
  val maxDepth: Int = 10

  val contextShift
    : ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded) // TODO: wkoszycki pass from F

  def applySnapshot(cbs: List[String]): F[Unit] =
    Sync[F].delay { logger.debug(s"[${dao.id.short}] applying snapshot for blocks: $cbs from others") }
      .flatMap(_ => cbs.map(memPool.remove).sequence.void)

  def fullData(key: String): F[Option[CheckpointCache]] =
    lookup(key).flatMap(_.map(convert(_)(dao)).sequence)

  def lookup(key: String): F[Option[CheckpointCacheMetadata]] =
    Lookup.extendedLookup[F, String, CheckpointCacheMetadata](List(memPool))(key)

  def contains(key: String): F[Boolean] = lookup(key).map(_.nonEmpty)

  def convert(merkle: CheckpointCacheMetadata)(implicit dao: DAO): F[CheckpointCache] =
    for {
      txs <- merkle.checkpointBlock.transactionsMerkleRoot.fold(List[Transaction]().pure[F])(fetchTransactions)
      msgs <- merkle.checkpointBlock.messagesMerkleRoot.fold(List[ChannelMessage]().pure[F])(fetchMessages)
      notifications <- merkle.checkpointBlock.notificationsMerkleRoot
        .fold(List[PeerNotification]().pure[F])(fetchNotifications)
    } yield
      CheckpointCache(
        CheckpointBlock(txs, merkle.checkpointBlock.checkpoint, msgs, notifications).some,
        merkle.children,
        merkle.height
      )

  def fetch[T, R](
    merkleRoot: String,
    service: MerkleStorageAlgebra[F, String, T],
    mapper: T => R,
    resolver: String => F[T]
  ): F[List[R]] =
    service
      .findHashesByMerkleRoot(merkleRoot)
      .map(
        _.get.map(
          hash =>
            service
              .lookup(hash)
              .flatMap(_.map(_.pure[F]).getOrElse(resolver(hash)).map(mapper))
        )
      )
      .map(_.toList.sequence)
      .flatten

  def fetchTransactions(merkleRoot: String)(implicit dao: DAO): F[List[Transaction]] =
    fetch[TransactionCacheData, Transaction](
      merkleRoot,
      transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveTransactionDefaults(s)(contextShift))
    )

  def fetchMessages(merkleRoot: String)(implicit dao: DAO): F[List[ChannelMessage]] =
    fetch[ChannelMessageMetadata, ChannelMessage](
      merkleRoot,
      messageService,
      (x: ChannelMessageMetadata) => x.channelMessage,
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveMessageDefaults(s)(contextShift))
    )

  def fetchNotifications(merkleRoot: String)(implicit dao: DAO): F[List[PeerNotification]] =
    fetch[PeerNotification, PeerNotification](
      merkleRoot,
      notificationService,
      (x: PeerNotification) => x,
      (s: String) => ???
    )

  def accept(checkpoint: FinishedCheckpoint)(implicit dao: DAO): F[Unit] = {

    val obtainPeers = dao.readyPeers.map { ready =>
      val filtered = ready.filter(t => checkpoint.facilitators.contains(t._1))
      (if (filtered.isEmpty) ready else filtered)
        .map(p => PeerApiClient(p._1, p._2.client))
        .toList
    }

    LiftIO[F].liftIO(dao.cluster.getNodeState).flatMap { nodeState =>
      (nodeState, checkpoint.checkpointCacheData.checkpointBlock) match {
        case (_, None) => Sync[F].raiseError[Unit](MissingCheckpointBlockException)
        case (NodeState.Ready, Some(cb)) =>
          val acceptance = for {
            _ <- syncPending(pendingAcceptanceFromOthers, cb.baseHash)
            _ <- Sync[F].delay { logger.debug(s"[${dao.id.short}] starting accept block: ${cb.baseHash} from others") }
            peers <- LiftIO[F].liftIO(obtainPeers)
            _ <- resolveMissingParents(cb, peers)
            _ <- accept(checkpoint.checkpointCacheData)
            _ <- pendingAcceptanceFromOthers.update(_.filterNot(_ == cb.baseHash))
          } yield ()

          acceptance.recoverWith {
            case ex: PendingAcceptance =>
              acceptErrorHandler(ex)
            case error =>
              pendingAcceptanceFromOthers.update(_.filterNot(_ == cb.baseHash)) *> acceptErrorHandler(error)
          }

          acceptance
        case (NodeState.DownloadCompleteAwaitingFinalSync, Some(_)) =>
          LiftIO[F].liftIO(dao.snapshotService.syncBufferAccept(checkpoint.checkpointCacheData))
        case (_, Some(_)) => Sync[F].raiseError[Unit](PendingDownloadException(dao.id))
      }
    }
  }

  def resolveMissingParents(cb: CheckpointBlock, peers: List[PeerApiClient], depth: Int = 1)(
    implicit dao: DAO
  ): F[List[CheckpointCache]] = {

    val checkError = if (depth >= maxDepth) {
      Sync[F].raiseError[Unit](new Exception("Max depth reached when resolving data."))
    } else Sync[F].unit

    val resolveSoe = cb.parentSOEBaseHashes() match {
      case List(_, _) => Sync[F].unit
      case _          => LiftIO[F].liftIO(DataResolver.resolveSoe(cb.parentSOEHashes.toList, peers)(contextShift).void)
    }

    val resolveCheckpoint = Sync[F]
      .delay(
        cb.parentSOEBaseHashes().toList
      )
      .map(
        parents => parents.traverse(h => contains(h).map(exist => (h, exist)))
      )
      .flatten
      .flatMap {
        case Nil =>
          Sync[F]
            .raiseError[List[CheckpointCache]](new RuntimeException("Soe hashes are empty even resolved previously"))
        case List((_, true), (_, true)) => Sync[F].pure(List[CheckpointCache]())
        case missing                    => LiftIO[F].liftIO(DataResolver.resolveCheckpoints(missing.map(_._1), peers)(contextShift))
      }

    for {
      _ <- checkError
      _ <- resolveSoe
      resolved <- resolveCheckpoint
      all <- resolved.traverse(c => resolveMissingParents(c.checkpointBlock.get, peers, depth + 1))
    } yield all.flatten

  }

  def accept(checkpoint: CheckpointCache)(implicit dao: DAO): F[Unit] = {

    val acceptCheckpoint: F[Unit] = checkpoint.checkpointBlock match {
      case None => Sync[F].raiseError[Unit](MissingCheckpointBlockException)

      case Some(cb) =>
        for {
          _ <- syncPending(pendingAcceptance, cb.baseHash)
          _ <- contains(cb.baseHash).ifM(
            dao.metrics
              .incrementMetricAsync[F]("checkpointAcceptBlockAlreadyStored") *> CheckpointAcceptBlockAlreadyStored(cb)
              .raiseError[F, Unit],
            Sync[F].unit
          )
          conflicts <- LiftIO[F].liftIO(CheckpointBlockValidatorNel.containsAlreadyAcceptedTx(cb))

          _ <- conflicts match {
            case Nil => Sync[F].unit
            case xs =>
              concurrentTipService
                .putConflicting(cb.baseHash, cb)
                .flatMap(_ => transactionService.removeConflicting(xs))
                .flatMap(_ => Sync[F].raiseError[Unit](TipConflictException(cb, conflicts)))
          }

          valid <- Sync[F].delay(cb.simpleValidation())
          _ <- if (!valid) Sync[F].raiseError[Unit](new Exception("CB to accept not valid")) else Sync[F].unit
          _ <- LiftIO[F].liftIO(cb.storeSOE())
          maybeHeight <- calculateHeight(checkpoint)

          height <- if (maybeHeight.isEmpty) {
            dao.metrics
              .incrementMetricAsync[F](Metrics.heightEmpty)
              .flatMap(_ => MissingHeightException(cb).raiseError[F, Height])
          } else Sync[F].pure(maybeHeight.get)

          _ <- memPool.put(cb.baseHash, checkpoint.copy(height = maybeHeight))
          _ <- Sync[F].delay(dao.recentBlockTracker.put(checkpoint.copy(height = maybeHeight)))
          _ <- acceptMessages(cb)
          _ <- acceptTransactions(cb, Some(checkpoint))
          _ <- updateRateLimiting(cb)
          _ <- Sync[F].delay {
            logger.debug(s"[${dao.id.short}] Accept checkpoint=${cb.baseHash}] and height $maybeHeight")
          }
          _ <- concurrentTipService.update(cb, height)
          _ <- LiftIO[F].liftIO(dao.snapshotService.updateAcceptedCBSinceSnapshot(cb))
          _ <- dao.metrics.incrementMetricAsync[F](Metrics.checkpointAccepted)
          _ <- incrementMetricIfDummy(cb)
          _ <- pendingAcceptance.update(_.filterNot(_ == cb.baseHash))
        } yield ()

    }

    acceptCheckpoint.recoverWith {
      case ex @ (PendingAcceptance(_) | MissingCheckpointBlockException) =>
        acceptErrorHandler(ex)
      case error =>
        pendingAcceptance.update(_.filterNot(_ == checkpoint.checkpointBlock.get.baseHash)) *> acceptErrorHandler(error)
    }
  }

  def acceptErrorHandler(err: Throwable): F[Unit] =
    err match {
      case knownError @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
        knownError.raiseError[F, Unit]
      case otherError =>
        Sync[F].delay(logger.error(s"Error when accepting block: ${otherError.getMessage}")) *> dao.metrics
          .incrementMetricAsync[F]("acceptCheckpoint_failure") *> otherError.raiseError[F, Unit]
    }

  private def incrementMetricIfDummy(checkpointBlock: CheckpointBlock) =
    if (checkpointBlock.transactions.forall(_.isDummy)) {
      dao.metrics.incrementMetricAsync[F]("checkpointsAcceptedWithDummyTxs")
    } else {
      Sync[F].unit
    }

  private def calculateHeight(checkpointCacheData: CheckpointCache)(implicit dao: DAO): F[Option[Height]] =
    Sync[F].delay {
      checkpointCacheData.checkpointBlock.flatMap { cb =>
        cb.calculateHeight() match {
          case None       => checkpointCacheData.height
          case calculated => calculated
        }
      }
    }

  private[storage] def syncPending(storage: SingleRef[F, Set[String]], baseHash: String)(implicit dao: DAO): F[Unit] =
    storage.update { hashes =>
      if (hashes.contains(baseHash)) {
        throw PendingAcceptance(baseHash)
      } else {
        hashes + baseHash
      }
    }

  private def acceptMessages(cb: CheckpointBlock)(implicit dao: DAO): F[List[Unit]] =
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

  def acceptTransactions(cb: CheckpointBlock, cpc: Option[CheckpointCache] = None)(implicit dao: DAO): F[Unit] = {
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
              dao.transactionService.accept(txMetadata, cpc) *> transferIfNotDummy(tx)
          }
          .void
      }

    insertTX
  }

  private def transferIfNotDummy(transaction: Transaction): IO[Unit] =
    if (!transaction.isDummy) dao.addressService.transfer(transaction).void else IO.unit

  private def updateRateLimiting(cb: CheckpointBlock): F[Unit] =
    rateLimiting.update(cb.transactions.toList)
}
