package org.constellation.checkpoint

import cats.effect.{Concurrent, ContextShift, IO, LiftIO}
import cats.implicits._
import org.constellation.domain.observation.Observation
import org.constellation.p2p.{DataResolver, PeerNotification}
import org.constellation.primitives.Schema.{CheckpointCache, _}
import org.constellation.primitives._
import org.constellation.storage.algebra.MerkleStorageAlgebra
import org.constellation.util.MerkleTree
import org.constellation.{ConstellationExecutionContext, DAO}

class CheckpointMerkleService[F[_]: Concurrent](
  dao: DAO,
  transactionService: MerkleStorageAlgebra[F, String, TransactionCacheData],
  messageService: MerkleStorageAlgebra[F, String, ChannelMessageMetadata],
  notificationService: MerkleStorageAlgebra[F, String, PeerNotification],
  observationService: MerkleStorageAlgebra[F, String, Observation]
) {

  val contextShift
    : ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded) // TODO: wkoszycki pass from F

  def storeMerkleRoots(data: CheckpointBlock): F[CheckpointBlockMetadata] =
    for {
      t <- store(data.transactions.map(_.hash), transactionService)
      m <- store(data.messages.map(_.signedMessageData.hash), messageService)
      n <- store(data.notifications.map(_.hash), notificationService)
      e <- store(data.observations.map(_.hash), observationService)
    } yield CheckpointBlockMetadata(t, data.checkpoint, m, n, e)

  private def store[A](data: Seq[String], ss: MerkleStorageAlgebra[F, String, A]): F[Option[String]] =
    data match {
      case Seq() => none[String].pure[F]
      case _ =>
        val rootHash = MerkleTree(data).rootHash
        ss.addMerkle(rootHash, data).map(_ => rootHash.some)
    }

  def convert(merkle: CheckpointCacheMetadata): F[CheckpointCache] =
    for {
      txs <- merkle.checkpointBlock.transactionsMerkleRoot.fold(List[Transaction]().pure[F])(fetchBatchTransactions)
      msgs <- merkle.checkpointBlock.messagesMerkleRoot.fold(List[ChannelMessage]().pure[F])(fetchMessages)
      notifications <- merkle.checkpointBlock.notificationsMerkleRoot
        .fold(List[PeerNotification]().pure[F])(fetchNotifications)
      obs <- merkle.checkpointBlock.observationsMerkleRoot.fold(List[Observation]().pure[F])(fetchBatchObservations)
    } yield
      CheckpointCache(
        CheckpointBlock(txs, merkle.checkpointBlock.checkpoint, msgs, notifications, obs).some,
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

  def fetchTransactions(merkleRoot: String): F[List[Transaction]] =
    fetch[TransactionCacheData, Transaction](
      merkleRoot,
      transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveTransactionDefaults(s)(contextShift)(dao = dao))
    )

  def fetchBatchTransactions(merkleRoot: String): F[List[Transaction]] =
    fetchBatch[TransactionCacheData, Transaction](
      merkleRoot,
      transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: List[String]) => LiftIO[F].liftIO(DataResolver.resolveBatchTransactionsDefaults(s)(contextShift)(dao = dao))
    )

  def fetchBatchObservations(merkleRoot: String): F[List[Observation]] =
    fetchBatch[Observation, Observation](
      merkleRoot,
      observationService,
      (o: Observation) => o,
      (s: List[String]) => LiftIO[F].liftIO(DataResolver.resolveBatchObservationsDefaults(s)(contextShift)(dao = dao))
    )

  def fetchMessages(merkleRoot: String): F[List[ChannelMessage]] =
    fetch[ChannelMessageMetadata, ChannelMessage](
      merkleRoot,
      messageService,
      (x: ChannelMessageMetadata) => x.channelMessage,
      (s: String) => LiftIO[F].liftIO(DataResolver.resolveMessageDefaults(s)(contextShift)(dao = dao))
    )

  def fetchNotifications(merkleRoot: String): F[List[PeerNotification]] =
    fetch[PeerNotification, PeerNotification](
      merkleRoot,
      notificationService,
      (x: PeerNotification) => x,
      (s: String) => ???
    )

}
