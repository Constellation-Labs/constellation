package org.constellation.domain.transaction

import cats.effect.{Bracket, Concurrent}
import cats.effect.concurrent.Semaphore
import cats.implicits._
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.PendingMemPool

class PendingTransactionsMemPool[F[_]: Concurrent](
  transactionChainService: TransactionChainService[F],
  semaphore: Semaphore[F]
) extends PendingMemPool[F, String, TransactionCacheData](semaphore) {

  // TODO: Rethink - use queue
  def pull(maxCount: Int): F[Option[List[TransactionCacheData]]] =
    Bracket[F, Throwable].bracket(ref.acquire)(
      _ =>
        ref.getUnsafe
          .flatMap(
            txs =>
              (if (txs.isEmpty) {
                 none[List[TransactionCacheData]].pure[F]
               } else {
                 sortForPull(txs.values.toList).flatMap { sorted =>
                   val (left, right) = sorted.splitAt(maxCount)
                   ref.unsafeModify(_ => (right.map(tx => tx.hash -> tx).toMap, Option(left).filter(_.nonEmpty)))
                 }
               })
          )
    )(_ => ref.release)

  private def sortForPull(txs: List[TransactionCacheData]): F[List[TransactionCacheData]] =
    txs
      .groupBy(_.transaction.src.address)
      .mapValues(_.sortBy(_.transaction.lastTxRef.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            transactionChainService
              .getLastAcceptedTransactionRef(hash)
              .map(_.ordinal == txs.headOption.map(_.transaction.lastTxRef.ordinal).getOrElse(-1))
              .ifM(
                txs.pure[F],
                List.empty[TransactionCacheData].pure[F]
              )
        }
      }
      .map(
        _.sortBy(_.map(-_.transaction.fee.getOrElse(0L)).sum).flatten
      )
}

object PendingTransactionsMemPool {

  def apply[F[_]: Concurrent](transactionChainService: TransactionChainService[F], semaphore: Semaphore[F]) =
    new PendingTransactionsMemPool[F](transactionChainService, semaphore)
}
