package org.constellation.domain.transaction

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.primitives.Schema.TransactionEdgeData
import org.constellation.primitives.{Edge, Transaction}

class TransactionChainService[F[_]: Concurrent] {

  // TODO: Make sure to clean-up those properly
  private[domain] val lastTransactionRef: Ref[F, Map[String, LastTransactionRef]] = Ref.unsafe(Map.empty)

  def getLastTransactionRef(address: String): F[LastTransactionRef] =
    lastTransactionRef.get.map(_.getOrElse(address, LastTransactionRef.empty))

  def setLastTransaction(edge: Edge[TransactionEdgeData], isDummy: Boolean): F[Transaction] = {
    val address = edge.observationEdge.parents.head.hash

    lastTransactionRef.modify { m =>
      val ref = m.getOrElse(address, LastTransactionRef.empty)
      val tx = Transaction(edge, ref, isDummy)
      (m + (address -> LastTransactionRef(tx.hash, ref.ordinal + 1)), tx)
    }
  }

}

object TransactionChainService {
  def apply[F[_]: Concurrent]: TransactionChainService[F] = new TransactionChainService[F]()
}
