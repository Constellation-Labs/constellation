package org.constellation.domain.transaction

import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import cats.implicits._
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.PendingMemPool

class PendingTransactionsMemPool[F[_]: Concurrent](semaphore: Semaphore[F])
    extends PendingMemPool[F, String, TransactionCacheData](semaphore) {

  // TODO: Rethink - use queue
  def pull(maxCount: Int): F[Option[List[TransactionCacheData]]] =
    ref.modify {
      case txs if txs.isEmpty =>
        (txs, none[List[TransactionCacheData]])
      case txs =>
        val sorted = sortForPull(txs.values.toList)
        val (left, right) = sorted.splitAt(maxCount)
        (right.map(tx => tx.hash -> tx).toMap, left.some)
    }

  private def sortForPull(txs: List[TransactionCacheData]): List[TransactionCacheData] =
    txs
      .groupBy(_.transaction.src.address)
      .mapValues(_.sortBy(_.transaction.lastTxRef.ordinal))
      .values
      .toSeq
      .sortBy(_.map(-_.transaction.fee.getOrElse(0L)).sum)
      .toList
      .flatten
}
