package org.constellation.checkpoint

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.transaction.TransactionService
import org.constellation.p2p.{DataResolver, PeerNotification}
import org.constellation.primitives.Schema.{CheckpointCache, _}
import org.constellation.primitives._
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.storage.{MessageService, NotificationService, ObservationService, SOEService}
import org.constellation.{ConstellationExecutionContext, DAO}

class CheckpointService[F[_]: Concurrent](
  dao: DAO,
  transactionService: TransactionService[F],
  messageService: MessageService[F],
  notificationService: NotificationService[F],
  observationService: ObservationService[F]
) {

  private[checkpoint] val memPool = new CheckpointBlocksMemPool[F](
    dao,
    transactionService.merklePool,
    messageService.merklePool,
    notificationService.merklePool,
    observationService.merklePool
  )

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  val contextShift
    : ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded) // TODO: wkoszycki pass from F

  def update(
    baseHash: String,
    update: CheckpointCacheMetadata => CheckpointCacheMetadata
  ): F[Option[CheckpointCacheMetadata]] =
    memPool.update(baseHash, update)

  def put(cbCache: CheckpointCache): F[CheckpointCacheMetadata] =
    memPool.put(cbCache.checkpointBlock.get.baseHash, cbCache)

  def applySnapshot(cbs: List[String]): F[Unit] =
    logger
      .debug(s"[${dao.id.short}] applying snapshot for blocks: $cbs from others")
      .flatMap(_ => cbs.map(memPool.remove).sequence.void)

  def fullData(key: String): F[Option[CheckpointCache]] =
    lookup(key).flatMap(_.map(convert(_)(dao)).sequence)

  def lookup(key: String): F[Option[CheckpointCacheMetadata]] =
    Lookup.extendedLookup[F, String, CheckpointCacheMetadata](List(memPool))(key)

  def contains(key: String): F[Boolean] = lookup(key).map(_.nonEmpty)

  def convert(merkle: CheckpointCacheMetadata)(implicit dao: DAO): F[CheckpointCache] =
    for {
      txs <- merkle.checkpointBlock.transactionsMerkleRoot.fold(List[Transaction]().pure[F])(fetchBatchTransactions)
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

  def fetchBatch[T, R](
    merkleRoot: String,
    service: MerkleStorageAlgebra[F, String, T],
    mapper: T => R,
    resolver: List[String] => F[List[T]]
  ): F[List[R]] =
    for {
      hashes <- service.findHashesByMerkleRoot(merkleRoot)
      lookupForHashes <- hashes.get.toList.traverse(hash => service.lookup(hash).map((hash, _)))
      resolvedByLookup = lookupForHashes.filter(_._2.isDefined).map(_._2.get)
      notResolvedByLookup = lookupForHashes.filter(_._2.isEmpty).map(_._1)
      resolvedByFetch <- resolver(notResolvedByLookup)
    } yield resolvedByFetch.map(mapper) ++ resolvedByLookup.map(mapper)

  def fetchTransactions(merkleRoot: String)(implicit dao: DAO): F[List[Transaction]] =
    fetch[TransactionCacheData, Transaction](
      merkleRoot,
      transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveTransactionDefaults(s)(contextShift))
    )

  def fetchBatchTransactions(merkleRoot: String)(implicit dao: DAO): F[List[Transaction]] =
    fetchBatch[TransactionCacheData, Transaction](
      merkleRoot,
      transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: List[String]) => LiftIO[F].liftIO(DataResolver.resolveBatchTransactionsDefaults(s)(contextShift))
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

}
