package org.constellation.domain.checkpointBlock

import cats.effect.{Concurrent, Sync}
import cats.implicits._
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

  private def checkOrdinal[F[_]: Concurrent](tx: Transaction)(
    transactionChainService: TransactionChainService[F]
  ): F[Option[Transaction]] =
    transactionChainService
      .getLastAcceptedTransactionRef(tx.src.address)
      .map(_.ordinal >= tx.ordinal)
      .ifM(Sync[F].pure(tx.some), Sync[F].pure(None))
}
