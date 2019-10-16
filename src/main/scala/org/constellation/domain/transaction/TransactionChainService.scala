package org.constellation.domain.transaction

import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.primitives.Transaction

class TransactionChainService[F[_]: Concurrent] {
  // TODO: Make sure to clean-up those properly
  private[domain] val lastTransactionRef: Ref[F, Map[String, LastTransactionRef]] = Ref.unsafe(Map.empty)

  def getLastTransactionRef(address: String): F[LastTransactionRef] =
    lastTransactionRef.get.map(_.getOrElse(address, LastTransactionRef.empty))

  def setLastTransaction(tx: Transaction): F[LastTransactionRef] =
    getLastTransactionRef(tx.src.address)
      .map(tx.lastTxRef.ordinal == _.ordinal + 1)
      .ifM(
        {
          val address = tx.src.address
          val ref = LastTransactionRef(tx.hash, tx.lastTxRef.ordinal)
          lastTransactionRef.modify { m =>
            (m + (address -> ref), m + (address -> ref))
          }.map(_.getOrElse(tx.src.address, LastTransactionRef.empty))
        },
        Sync[F].raiseError[LastTransactionRef](new RuntimeException("Created transaction has wrong ordinal number"))
      )

}

object TransactionChainService {
  def apply[F[_]: Concurrent]: TransactionChainService[F] = new TransactionChainService[F]()
}
