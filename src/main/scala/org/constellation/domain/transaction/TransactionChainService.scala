package org.constellation.domain.transaction

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.consensus.SnapshotInfo
import org.constellation.primitives.Schema.TransactionEdgeData
import org.constellation.primitives.{Edge, Transaction}

class TransactionChainService[F[_]: Concurrent] {

  // TODO: Make sure to clean-up those properly
  private[domain] val lastTransactionRef: Ref[F, Map[String, LastTransactionRef]] = Ref.unsafe(Map.empty)
  private[domain] val lastAcceptedTransactionRef: Ref[F, Map[String, LastTransactionRef]] = Ref.unsafe(Map.empty)

  def getLastTransactionRef(address: String): F[LastTransactionRef] =
    lastTransactionRef.get.map(_.getOrElse(address, LastTransactionRef.empty))

  def getLastAcceptedTransactionRef(address: String): F[LastTransactionRef] =
    lastAcceptedTransactionRef.get.map(_.getOrElse(address, LastTransactionRef.empty))

  def getLastAcceptedTransactionMap(): F[Map[String, LastTransactionRef]] = lastAcceptedTransactionRef.get

  def acceptTransaction(tx: Transaction): F[Unit] =
    lastAcceptedTransactionRef.modify { m =>
      val address = tx.src.address
      if (tx.isDummy) (m, ())
      else (m + (address -> LastTransactionRef(tx.hash, tx.ordinal)), ())
    }

  def setLastTransaction(edge: Edge[TransactionEdgeData], isTest: Boolean): F[Transaction] = {
    val address = edge.parents.head.hash
    lastTransactionRef.modify { m =>
      val ref = m.getOrElse(address, LastTransactionRef.empty)
      val tx = Transaction(edge, ref, false, isTest)
      (m + (address -> LastTransactionRef(tx.hash, tx.ordinal)), tx)
    }
  }

  def checkDummyTransaction(edge: Edge[TransactionEdgeData], isTest: Boolean): F[Transaction] = {
    val address = edge.parents.head.hash
    lastTransactionRef.get.map { m =>
      val ref = m.get(address)
      Transaction(edge, LastTransactionRef.empty, true, isTest)
    }
  }

  def applySnapshotInfo(snapshotInfo: SnapshotInfo): F[Unit] =
    lastAcceptedTransactionRef.modify { _ =>
      (snapshotInfo.lastAcceptedTransactionRef, ())
    }

}

object TransactionChainService {
  def apply[F[_]: Concurrent]: TransactionChainService[F] = new TransactionChainService[F]()
}
