package org.constellation.domain.transaction

import cats.effect.{Bracket, Concurrent}
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.primitives.Schema.Address
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.{PendingMemPool, RateLimiting}

import scala.annotation.tailrec

class PendingTransactionsMemPool[F[_]: Concurrent](
  transactionChainService: TransactionChainService[F],
  rateLimiting: RateLimiting[F]
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
      .toList
      .traverse {
        case (address, aTxs) =>
          for {
            isBelowLimit <- rateLimiting.available(Address(address)).map(_ > 0)
            (positiveFee, noFee) = aTxs.partition(_.transaction.fee.exists(_ > 0L))
            selectedTxs = if (isBelowLimit) positiveFee ++ noFee else positiveFee
          } yield (address, selectedTxs)
      }
      .flatMap { t =>
        t.traverse {
          case (addressHash, txs) =>
            transactionChainService
              .getLastAcceptedTransactionRef(addressHash)
              .map { last =>
                val consecutiveTransactions = takeConsecutiveTransactions(last, txs)
                (last, consecutiveTransactions)
              }
              .flatTap {
                case (last, consecTxs) =>
                  logger.info(s"${Console.YELLOW}Last accepted: ${last} | Txs head: ${consecTxs.headOption
                    .map(_.transaction.lastTxRef)} hash=${consecTxs.headOption.map(_.transaction.hash)}${Console.RESET}")
              }
              .map { case (_, consecTxs) => consecTxs }
        }
      }
      .map(
        _.sortBy(_.map(-_.transaction.fee.getOrElse(0L)).sum).flatten
      )

  private def takeConsecutiveTransactions(
    lastAcceptedTxRef: LastTransactionRef,
    txs: List[TransactionCacheData]
  ): List[TransactionCacheData] = {
    @tailrec
    def loop(
      acc: List[TransactionCacheData],
      prevTxRef: LastTransactionRef,
      txs: List[TransactionCacheData]
    ): List[TransactionCacheData] =
      txs.find(_.transaction.lastTxRef == prevTxRef) match {
        case Some(tx) =>
          loop(tx +: acc, LastTransactionRef(tx.transaction.hash, tx.transaction.ordinal), txs.diff(List(tx)))
        case None => acc.reverse //to preserve order of the chain
      }

    loop(List.empty, lastAcceptedTxRef, txs)
  }
}

object PendingTransactionsMemPool {

  def apply[F[_]: Concurrent](transactionChainService: TransactionChainService[F], rateLimiting: RateLimiting[F]) =
    new PendingTransactionsMemPool[F](transactionChainService, rateLimiting)
}
