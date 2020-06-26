package org.constellation.checkpoint

import cats.effect.{Concurrent, ContextShift, IO}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.observation.Observation
import org.constellation.p2p.{DataResolver, PeerNotification}
import org.constellation.primitives.Schema.{CheckpointCache, _}
import org.constellation.primitives._
import org.constellation.storage.algebra.MerkleStorageAlgebra
import org.constellation.util.MerkleTree
import org.constellation.{ConstellationExecutionContext, DAO}

class CheckpointMerkleService[F[_]: Concurrent](
  transactionService: MerkleStorageAlgebra[F, String, TransactionCacheData],
  notificationService: MerkleStorageAlgebra[F, String, PeerNotification],
  observationService: MerkleStorageAlgebra[F, String, Observation],
  dataResolver: DataResolver[F]
) {

  private val logger = Slf4jLogger.getLogger[F]

  def storeMerkleRoots(data: CheckpointBlock): F[CheckpointBlockMetadata] =
    for {
      t <- store(data.transactions.map(_.hash), transactionService)
      n <- store(data.notifications.map(_.hash), notificationService)
      o <- store(data.observations.map(_.hash), observationService)
    } yield CheckpointBlockMetadata(t, data.checkpoint, None, n, o)

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
      notifications <- merkle.checkpointBlock.notificationsMerkleRoot
        .fold(List[PeerNotification]().pure[F])(fetchNotifications)
      obs <- merkle.checkpointBlock.observationsMerkleRoot.fold(List[Observation]().pure[F])(fetchBatchObservations)
    } yield
      CheckpointCache(
        CheckpointBlock(txs, merkle.checkpointBlock.checkpoint, Seq.empty, notifications, obs),
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

  def fetchBatchTransactions(merkleRoot: String): F[List[Transaction]] =
    fetchBatch[TransactionCacheData, Transaction](
      merkleRoot,
      transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: List[String]) => dataResolver.resolveBatchTransactionsDefaults(s)
    ).handleErrorWith(
      e => logger.error(e)("Fetch batch for merkle root error: fetchBatchTransactions").map(_ => List.empty)
    )

  def fetchBatchObservations(merkleRoot: String): F[List[Observation]] =
    fetchBatch[Observation, Observation](
      merkleRoot,
      observationService,
      (o: Observation) => o,
      (s: List[String]) => dataResolver.resolveBatchObservationsDefaults(s)
    ).handleErrorWith(
      e => logger.error(e)("Fetch batch for merkle root error:fetchBatchObservations").map(_ => List.empty)
    )

  def fetchNotifications(merkleRoot: String): F[List[PeerNotification]] =
    fetch[PeerNotification, PeerNotification](
      merkleRoot,
      notificationService,
      (x: PeerNotification) => x,
      (s: String) => ???
    ).handleErrorWith(
      e => logger.error(e)("Fetch batch for merkle root error: fetchNotifications").map(_ => List.empty)
    )

}
