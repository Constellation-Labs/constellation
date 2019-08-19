package org.constellation.storage

import cats.effect._
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.Logger
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.transactions.PendingTransactionsMemPool
import org.constellation.DAO

class TransactionService[F[_]: Concurrent: Logger](dao: DAO) extends ConsensusService[F, TransactionCacheData] {

  protected[storage] val pending = new PendingTransactionsMemPool[F]()

  override def accept(tx: TransactionCacheData): F[Unit] =
    super
      .accept(tx)
      .flatTap(_ => Sync[F].delay(dao.metrics.incrementMetric("transactionAccepted")))

  def applySnapshot(txs: List[TransactionCacheData], merkleRoot: String): F[Unit] =
    withLock("merklePoolUpdate", merklePool.remove(merkleRoot)) *>
      txs.traverse(tx => withLock("acceptedUpdate", accepted.remove(tx.hash))).void

  def applySnapshot(merkleRoot: String): F[Unit] =
    findHashesByMerkleRoot(merkleRoot).flatMap(tx => withLock("acceptedUpdate", accepted.remove(tx.toSet.flatten))) *>
      withLock("merklePoolUpdate", merklePool.remove(merkleRoot))

  override def pullForConsensus(maxCount: Int): F[List[TransactionCacheData]] =
    count(status = ConsensusStatus.Pending).flatMap {
      case 0L => dummyTransaction(1)
      case _  => super.pullForConsensus(maxCount)
    }

  def dummyTransaction(count: Int): F[List[TransactionCacheData]] =
    List
      .fill(count)(
        createDummyTransaction(dao.selfAddressStr, KeyUtils.makeKeyPair().getPublic.toId.address, dao.keyPair)
      )
      .map(TransactionCacheData(_))
      .traverse(tx => inConsensus.put(tx.hash, tx))

  def removeConflicting(txs: List[String]): F[Unit] =
    pending.remove(txs.toSet) *> unknown.remove(txs.toSet)
}
