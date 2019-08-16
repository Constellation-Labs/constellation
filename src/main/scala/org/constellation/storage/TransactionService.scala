package org.constellation.storage

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.Logger
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.transactions.PendingTransactionsMemPool
import org.constellation.DAO
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.storage.ConsensusStatus.ConsensusStatus

class TransactionService[F[_]: Concurrent: Logger](transactionChainService: TransactionChainService[F], dao: DAO)
    extends ConsensusService[F, TransactionCacheData] {

  protected[storage] val pending = new PendingTransactionsMemPool[F](Semaphore.in[IO, F](1).unsafeRunSync())

  override def accept(tx: TransactionCacheData, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(tx, cpc)
      .flatMap(_ => transactionChainService.observeTransaction(tx.transaction.src.address, tx.transaction.hash))
      .void
      .flatTap(_ => Sync[F].delay(dao.metrics.incrementMetric("transactionAccepted")))
      .flatTap(_ => Logger[F].debug(s"Accepting transaction=${tx.hash}"))

  def applySnapshot(txs: List[TransactionCacheData], merkleRoot: String): F[Unit] =
    withLock("merklePoolUpdate", merklePool.remove(merkleRoot)) >>
      txs.traverse(tx => withLock("acceptedUpdate", accepted.remove(tx.hash))).void

  def applySnapshot(merkleRoot: String): F[Unit] =
    findHashesByMerkleRoot(merkleRoot).flatMap(tx => withLock("acceptedUpdate", accepted.remove(tx.toSet.flatten))) >>
      withLock("merklePoolUpdate", merklePool.remove(merkleRoot))

  override def pullForConsensus(maxCount: Int): F[List[TransactionCacheData]] =
    count(status = ConsensusStatus.Pending).flatMap {
      case 0L => dummyTransaction(1)
      case _  => super.pullForConsensus(maxCount)
    }

  override def put(
    tx: TransactionCacheData,
    as: ConsensusStatus,
    cpc: Option[CheckpointCache] = None
  ): F[TransactionCacheData] =
    as match {
      case ConsensusStatus.Pending =>
        for {
          t <- transactionChainService.getNext(tx.transaction.src.address)
          result <- super
            .put(tx.copy(transaction = tx.transaction.copy(previousHash = t._1, count = t._2)), as, cpc)
        } yield result

      case _ => super.put(tx, as, cpc)
    }

  def dummyTransaction(count: Int): F[List[TransactionCacheData]] =
    List
      .fill(count)(
        createDummyTransaction(dao.selfAddressStr, KeyUtils.makeKeyPair().getPublic.toId.address, dao.keyPair)
      )
      .map(TransactionCacheData(_))
      .traverse(tx => inConsensus.put(tx.hash, tx))

  def removeConflicting(txs: List[String]): F[Unit] =
    pending.remove(txs.toSet) >> unknown.remove(txs.toSet)
}
