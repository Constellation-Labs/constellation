package org.constellation.storage

import cats.effect.{IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCache, CheckpointCacheMetadata, Height}
import org.constellation.primitives._
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.util.{MerkleTree, Metrics}
import constellation._

class CheckpointBlocksMemPool[F[_]: Sync](
  dao: DAO,
  transactionsMerklePool: StorageService[F, Seq[String]],
  messagesMerklePool: StorageService[F, Seq[String]],
  notificationsMerklePool: StorageService[F, Seq[String]]
) extends StorageService[F, CheckpointCacheMetadata]() {

  def put(
    key: String,
    value: CheckpointCache
  ): F[CheckpointCacheMetadata] =
    value.checkpointBlock
      .map(cb => incrementChildrenCount(cb.parentSOEBaseHashes()(dao)))
      .sequence *>
      storeMerkleRoots(value.checkpointBlock.get)
        .flatMap(ccm => {
          super.put(key, CheckpointCacheMetadata(ccm, value.children, value.height))
        })

  def storeMerkleRoots(data: CheckpointBlock): F[CheckpointBlockMetadata] =
    for {
      t <- store(data.transactions.map(_.hash), transactionsMerklePool)
      m <- store(data.messages.map(_.signedMessageData.hash), messagesMerklePool)
      n <- store(data.notifications.map(_.hash), notificationsMerklePool)
    } yield CheckpointBlockMetadata(t, data.checkpoint, m, n)

  private def store(data: Seq[String], ss: StorageService[F, Seq[String]]): F[Option[String]] =
    data match {
      case Seq() => none[String].pure[F]
      case _ =>
        val rootHash = MerkleTree(data).rootHash
        ss.put(rootHash, data).map(_ => rootHash.some)
    }

  def incrementChildrenCount(hashes: Seq[String]): F[Unit] =
    hashes.toList.map { hash =>
      update(hash, (cd: CheckpointCacheMetadata) => cd.copy(children = cd.children + 1))
    }.sequence.void
}

class CheckpointService[F[_]: Sync: LiftIO](
  dao: DAO,
  transactionService: TransactionService[F],
  messageService: MessageService[F],
  notificationService: NotificationService[F],
  concurrentTipService: ConcurrentTipService
) extends StrictLogging {

  val memPool = new CheckpointBlocksMemPool[F](
    dao,
    transactionService.merklePool,
    messageService.merklePool,
    notificationService.merklePool
  )

  val pendingAcceptance = new StorageService[F, CheckpointBlock](Some(10))

  def applySnapshot(cbs: List[String]): F[Unit] =
    cbs.map(memPool.remove).sequence.void

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
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveTransactionsDefaults(s).map(_.get))
    )

  def fetchMessages(merkleRoot: String)(implicit dao: DAO): F[List[ChannelMessage]] =
    fetch[ChannelMessageMetadata, ChannelMessage](
      merkleRoot,
      messageService,
      (x: ChannelMessageMetadata) => x.channelMessage,
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveMessagesDefaults(s).map(_.get))
    )

  def fetchNotifications(merkleRoot: String)(implicit dao: DAO): F[List[PeerNotification]] =
    fetch[PeerNotification, PeerNotification](
      merkleRoot,
      notificationService,
      (x: PeerNotification) => x,
      (s: String) => ???
    )

  def accept(checkpoint: CheckpointCache)(implicit dao: DAO): F[Unit] = {

    val acceptCheckpoint: F[Unit] = checkpoint.checkpointBlock match {
      case None => Sync[F].raiseError(MissingCheckpointBlockException)

      case Some(cb) if dao.checkpointService.contains(cb.baseHash).unsafeRunSync() =>
        for {
          _ <- dao.metrics.incrementMetricAsync[F]("checkpointAcceptBlockAlreadyStored")
          _ <- CheckpointAcceptBlockAlreadyStored(cb).raiseError[F, Unit]
        } yield ()

      case Some(cb) =>
        for {
          _ <- syncPending(cb)

          conflicts <- LiftIO[F].liftIO(CheckpointBlockValidatorNel.containsAlreadyAcceptedTx(cb))

          _ <- conflicts match {
            case Nil => Sync[F].unit
            case _ =>
              LiftIO[F].liftIO {
                concurrentTipService
                  .putConflicting(cb.baseHash, cb)
                  .flatMap(_ => IO.raiseError(TipConflictException(cb, conflicts)))
                  .void
              }
          }

          _ <- LiftIO[F].liftIO(cb.storeSOE())
          maybeHeight <- calculateHeight(checkpoint)

          _ <- if (maybeHeight.isEmpty) {
            dao.metrics
              .incrementMetricAsync[F](Metrics.heightEmpty)
              .flatMap(_ => MissingHeightException(cb).raiseError[F, Unit])
              .void
          } else Sync[F].unit

          _ <- memPool.put(cb.baseHash, checkpoint.copy(height = maybeHeight))
          _ <- Sync[F].delay(dao.recentBlockTracker.put(checkpoint.copy(height = maybeHeight)))
          _ <- acceptMessages(cb)
          _ <- acceptTransactions(cb)
          _ <- Sync[F].delay { logger.debug(s"[${dao.id.short}] Accept checkpoint=${cb.baseHash}]") }
          _ <- LiftIO[F].liftIO(concurrentTipService.update(cb))
          _ <- LiftIO[F].liftIO(dao.snapshotService.updateAcceptedCBSinceSnapshot(cb))
          _ <- dao.metrics.incrementMetricAsync[F](Metrics.checkpointAccepted)
          _ <- pendingAcceptance.remove(cb.baseHash)
        } yield ()

    }

    acceptCheckpoint.recoverWith {
      case err =>
        dao.metrics.incrementMetricAsync[F]("acceptCheckpoint_failure") *> err.raiseError[F, Unit]
    }
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

  private def syncPending(cb: CheckpointBlock)(implicit dao: DAO): F[Unit] =
    Sync[F].delay {
      pendingAcceptance.synchronized {
        pendingAcceptance.contains(cb.baseHash).flatMap {
          case false => pendingAcceptance.put(cb.baseHash, cb)
          case _     => throw PendingAcceptance(cb)
        }
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

  private def acceptTransactions(cb: CheckpointBlock)(implicit dao: DAO): F[Unit] = {
    def toCacheData(tx: Transaction) = TransactionCacheData(
      tx,
      valid = true,
      inMemPool = false,
      inDAG = true,
      Map(cb.baseHash -> true),
      resolved = true,
      cbBaseHash = Some(cb.baseHash)
    )

    val insertTX =
      LiftIO[F].liftIO {
        cb.transactions.toList
          .map(tx ⇒ (tx, toCacheData(tx)))
          .traverse {
            case (tx, txMetadata) =>
              dao.transactionService.accept(txMetadata) *>
                dao.addressService.transfer(tx)
          }
          .void
      }

    Sync[F].delay { logger.debug(s"Accepting transactions ${cb.transactions.size}") } >> insertTX
  }
}
