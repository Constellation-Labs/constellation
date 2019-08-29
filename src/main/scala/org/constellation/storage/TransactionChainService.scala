package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.primitives.concurrency.SingleRef

class TransactionChainService[F[_]: Concurrent: Logger] {
  // TODO: Make sure to clean-up those properly
  private[storage] val lastTransactionCount: SingleRef[F, Long] = SingleRef(0)
  private[storage] val addressCount: SingleRef[F, Map[String, Long]] = SingleRef(Map.empty[String, Long])
  private[storage] val lastTxHash: SingleRef[F, Map[String, String]] = SingleRef(Map.empty[String, String])

  def getNext(address: String): F[(String, Long)] =
    lastTransactionCount
      .modify(count => (count + 1, count + 1))
      .flatMap(count => lastTransactionHash(address).map(_.getOrElse("")).map(a => (a, count)))

  def getLatest(address: String): F[Long] =
    addressCount.get
      .map(_.getOrElse(address, 0))

  def lastTransactionHash(address: String): F[Option[String]] =
    lastTxHash.get
      .map(_.get(address))

  def observeTransaction(address: String, hash: String): F[(Long, String)] =
    incrementAddress(address)
      .flatMap(c => setLastTransactionHash(address, hash).map(h => (c, h)))

  private def incrementAddress(address: String): F[Long] =
    addressCount.unsafeModify { m =>
      val a = m ++ Map(address -> (m.getOrElse(address, 0L) + 1L))
      (a, a.getOrElse(address, 0))
    }

  def setLastTransactionHash(address: String, hash: String): F[String] =
    lastTxHash
      .unsafeModify(m => (m ++ Map(address -> hash), hash))
}

object TransactionChainService {
  def apply[F[_]: Concurrent: Logger]: TransactionChainService[F] = new TransactionChainService[F]()
}
