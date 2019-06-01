package org.constellation.storage.transactions

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.algebra.LookupAlgebra

class PendingTransactionsMemPool[F[_]: Sync]()
    extends LookupAlgebra[F, String, TransactionCacheData] {

  private val txRef: Ref[F, Map[String, TransactionCacheData]] =
    Ref.unsafe[F, Map[String, TransactionCacheData]](Map())

  def put(key: String, value: TransactionCacheData): F[TransactionCacheData] =
    txRef.get.flatMap(txs => txRef.set(txs + (key -> value)).map(_ => value))

  def lookup(key: String): F[Option[TransactionCacheData]] =
    txRef.get.map(_.find(_._2.transaction.hash == key).map(_._2))

  def contains(key: String): F[Boolean] =
    txRef.get.map(_.exists(_._2.transaction.hash == key))

  // TODO: Rethink - use queue
  def pull(minCount: Int): F[Option[List[TransactionCacheData]]] =
    txRef.get.flatMap { txs =>
      if (txs.size < minCount) none[List[TransactionCacheData]].pure[F]
      else {
        val (left, right) = txs.splitAt(minCount)
        txRef.set(right).map(_ => left.toList.map(_._2).some)
      }
    }

  def size(): F[Long] =
    txRef.get.map(_.size.toLong)

}
