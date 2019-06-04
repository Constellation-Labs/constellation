package org.constellation.storage

import cats.effect.{Async, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCache, CheckpointCacheMetadata}
import org.constellation.primitives._
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.util.MerkleTree

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

  private def store(data: Seq[String], ss: StorageService[F, Seq[String]]): F[Option[String]] = {
    data match {
      case Seq() => none[String].pure[F]
      case _ =>
        val rootHash = MerkleTree(data).rootHash
        ss.put(rootHash, data).map(_ => rootHash.some)
    }
  }

  def incrementChildrenCount(hashes: Seq[String]): F[Unit] =
    hashes
      .toList
      .map { hash =>
        update(hash, (cd: CheckpointCacheMetadata) => cd.copy(children = cd.children + 1))
      }
      .sequence
      .void
}

class CheckpointService[F[_]: Sync : LiftIO](
  dao: DAO,
  transactionService: TransactionService[F],
  messageService: MessageService[F],
  notificationService: NotificationService[F]
) extends StrictLogging {
  val memPool = new CheckpointBlocksMemPool[F](
    dao,
    transactionService.merklePool,
    messageService.merklePool,
    notificationService.merklePool)

  val pendingAcceptance = new StorageService[IO, CheckpointBlock](Some(10))

  def applySnapshot(cbs: List[String]): F[Unit] =
    cbs.map(memPool.remove)
      .sequence
      .void

  def fullData(key: String): F[Option[CheckpointCache]] =
    lookup(key).flatMap(_.map(convert(_)(dao)).sequence)

  def lookup(key: String): F[Option[CheckpointCacheMetadata]] =
    Lookup.extendedLookup[F, String, CheckpointCacheMetadata](List(memPool))(key)

  def contains(key: String): F[Boolean] = lookup(key).map(_.nonEmpty)

  def convert(merkle: CheckpointCacheMetadata)(implicit dao: DAO): F[CheckpointCache] = {
    for {
      txs <- merkle.checkpointBlock.transactionsMerkleRoot.fold(List[Transaction]().pure[F])(fetchTransactions)
      msgs <- merkle.checkpointBlock.messagesMerkleRoot.fold(List[ChannelMessage]().pure[F])(fetchMessages)
      notifications <- merkle.checkpointBlock.notificationsMerkleRoot
        .fold(List[PeerNotification]().pure[F])(fetchNotifications)
    } yield CheckpointCache(
      CheckpointBlock(txs, merkle.checkpointBlock.checkpoint, msgs, notifications).some,
      merkle.children,
      merkle.height)
  }

  def fetch[T, R](
    merkleRoot: String,
    service: MerkleStorageAlgebra[F, String, T],
    mapper: T => R,
    resolver: String => F[T],
  ): F[List[R]] =
    service.findHashesByMerkleRoot(merkleRoot)
      .map(_.get.map(hash =>
        service
          .lookup(hash)
          .flatMap(_.map(_.pure[F]).getOrElse(resolver(hash)).map(mapper))))
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

}
