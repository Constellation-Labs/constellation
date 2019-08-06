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
    withLock("merklePoolRemove", merklePool.remove(merkleRoot)) *>
      txs.traverse(tx => withLock("acceptedRemove", accepted.remove(tx.hash))).void

  def applySnapshot(merkleRoot: String): F[Unit] =
    findHashesByMerkleRoot(merkleRoot).flatMap(tx => withLock("acceptedRemove", accepted.remove(tx.toSet.flatten))) *>
      withLock("merklePoolRemove", merklePool.remove(merkleRoot))

  def pullForConsensusWithDummy(minCount: Int, roundId: String = "roundId"): F[List[TransactionCacheData]] =
    count(status = ConsensusStatus.Pending).flatMap {
      case 0L => dummyTransaction(minCount)
      case _  => pullForConsensus(minCount)
    }

  def dummyTransaction(minCount: Int): F[List[TransactionCacheData]] =
    List
      .fill(minCount)(
        TransactionCacheData(
          createDummyTransaction(dao.selfAddressStr, KeyUtils.makeKeyPair().getPublic.toId.address, dao.keyPair)
        )
      )
      .traverse(put)
      .flatMap(_ => pullForConsensus(minCount))
}
