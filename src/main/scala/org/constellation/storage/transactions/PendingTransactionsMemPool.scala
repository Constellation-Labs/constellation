package org.constellation.storage.transactions

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import org.constellation.primitives.TransactionCacheData
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage.PendingMemPool

class PendingTransactionsMemPool[F[_]: Concurrent]() extends PendingMemPool[F, String, TransactionCacheData] {

  // TODO: Rethink - use queue
  def pull(maxCount: Int): F[Option[List[TransactionCacheData]]] =
    ref.modify { txs =>
      if (txs.size < 1) {
        (txs, none[List[TransactionCacheData]])
      } else {
        val sorted = txs.toList.sortWith(_._2.transaction.edge.data.fee > _._2.transaction.edge.data.fee)
        val (left, right) = sorted.splitAt(maxCount)
        (right.toMap, left.map(_._2).some)
      }
    }

}
