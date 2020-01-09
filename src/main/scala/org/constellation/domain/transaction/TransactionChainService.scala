package org.constellation.domain.transaction

import java.security.KeyPair

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.consensus.SnapshotInfo
import org.constellation.domain.transaction.TransactionService.createTransactionEdge
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
      (m + (address -> LastTransactionRef(tx.hash, tx.ordinal)), ())
    }

  def createAndSetLastTransaction(src: String,
                                  dst: String,
                                  amount: Long,
                                  keyPair: KeyPair,
                                  isDummy: Boolean,
                                  fee: Option[Long] = None,
                                  normalized: Boolean = false): F[Transaction] =
    lastTransactionRef.modify { m =>
      val ref = m.getOrElse(src, LastTransactionRef.empty)
      val edge: Edge[TransactionEdgeData] = createTransactionEdge(src, dst, ref, amount, keyPair, fee, normalized)
      val address = edge.observationEdge.parents.head.hash
      val tx = Transaction(edge, ref, isDummy)
      (m + (address -> LastTransactionRef(tx.hash, tx.ordinal)), tx)
    }


  def applySnapshotInfo(snapshotInfo: SnapshotInfo): F[Unit] =
    lastAcceptedTransactionRef.modify { _ =>
      (snapshotInfo.lastAcceptedTransactionRef, ())
    }

}

object TransactionChainService {
  def apply[F[_]: Concurrent]: TransactionChainService[F] = new TransactionChainService[F]()
}
