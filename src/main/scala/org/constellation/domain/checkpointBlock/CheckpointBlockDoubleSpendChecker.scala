package org.constellation.domain.checkpointBlock

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import org.constellation.domain.transaction.TransactionChainService
import org.constellation.primitives.{CheckpointBlock, Transaction}

object CheckpointBlockDoubleSpendChecker {

  def check[F[_]: Concurrent](cb: CheckpointBlock)(
    transactionChainService: TransactionChainService[F]
  ): F[List[Transaction]] =
    cb.transactions.toList
      .pure[F]
      .flatMap(_.traverse(checkOrdinal(_)(transactionChainService)))
      .map(_.flatten)
      .map(_ ++ checkTxsRefsIn(cb))

  def checkTxsRefsIn(cb: CheckpointBlock): List[Transaction] =
    cb.transactions
      .groupBy(_.src.address)
      .toList
      .flatMap {
        case (_: String, txs: List[Transaction]) =>
          txs.groupBy(_.ordinal).toList.flatMap {
            case (_, txs: List[Transaction]) =>
              if (txs.size > 1) txs
              else List.empty
          }
        case (_, _) => List.empty
      }

  private def checkOrdinal[F[_]: Concurrent](tx: Transaction)(
    transactionChainService: TransactionChainService[F]
  ): F[Option[Transaction]] =
    transactionChainService
      .getLastAcceptedTransactionRef(tx.src.address)
      .map(_.ordinal >= tx.ordinal)
      .ifM(Sync[F].pure(tx.some), Sync[F].pure(None))
}
