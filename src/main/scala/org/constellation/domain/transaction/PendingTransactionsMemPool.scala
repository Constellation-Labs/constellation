package org.constellation.domain.transaction

import cats.effect.{Bracket, Concurrent}
import cats.effect.concurrent.Semaphore
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.PendingMemPool

class PendingTransactionsMemPool[F[_]: Concurrent](
  transactionChainService: TransactionChainService[F]
) extends PendingMemPool[F, String, TransactionCacheData] {

  val logger = Slf4jLogger.getLogger[F]

  private val semaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()

  // TODO: Rethink - use queue
  def pull(maxCount: Int): F[Option[List[TransactionCacheData]]] =
    Bracket[F, Throwable].bracket(semaphore.acquire)(
      _ =>
        ref.get
          .flatMap(
            txs =>
              if (txs.isEmpty) {
                none[List[TransactionCacheData]].pure[F]
              } else {
                sortAndFilterForPull(txs.values.toList).flatMap { sorted =>
                  val (toUse, _) = sorted.splitAt(maxCount)
                  val hashesToUse = toUse.map(_.hash)
                  ref.modify { txsA =>
                    val leftTxs = txsA.filterKeys(!hashesToUse.contains(_))
                    (leftTxs, Option(toUse).filter(_.nonEmpty))
                  }
                }
              }
          )
    )(_ => semaphore.release)

  private def sortAndFilterForPull(txs: List[TransactionCacheData]): F[List[TransactionCacheData]] =
    txs
      .groupBy(_.transaction.src.address)
      .mapValues(_.sortBy(_.transaction.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            transactionChainService
              .getLastAcceptedTransactionRef(hash)
              .flatTap { last =>
                logger.info(s"${Console.YELLOW}Last accepted: ${last} | Txs head: ${txs.headOption
                  .map(_.transaction.lastTxRef)} hash=${txs.headOption.map(_.transaction.hash)}${Console.RESET}")
              }
              .map { last =>
                val ordinal = last.ordinal
                val left = txs.filter(t => t.transaction.lastTxRef.ordinal >= ordinal || t.transaction.isTest) // TODO: get rid of >=
                (last, left)
              }
              .map {
                case (last, t) =>
                  if (t.headOption.map(_.transaction.lastTxRef).contains(last)) t
                  else List.empty[TransactionCacheData]
              }
        }
      }
      .map(
        _.sortBy(_.map(-_.transaction.fee.getOrElse(0L)).sum).flatten
      )
}

object PendingTransactionsMemPool {

  def apply[F[_]: Concurrent](transactionChainService: TransactionChainService[F]) =
    new PendingTransactionsMemPool[F](transactionChainService)
}
