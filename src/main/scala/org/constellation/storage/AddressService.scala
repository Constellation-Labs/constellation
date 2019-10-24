package org.constellation.storage

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.Transaction
import org.constellation.primitives.concurrency.MultiLock

class AddressService[F[_]: Concurrent]() {

  private val locks = new MultiLock[F, String]()

  private val memPool = new StorageService[F, AddressCacheData]("address_mem_pool".some)

  def toMap: F[Map[String, AddressCacheData]] = memPool.toMap()

  def putUnsafe(key: String, value: AddressCacheData): F[AddressCacheData] = memPool.put(key, value)

  def size: F[Long] = memPool.size()

  def lookup(key: String): F[Option[AddressCacheData]] =
    memPool.lookup(key)

  def transfer(tx: Transaction): F[AddressCacheData] =
    locks.acquire(List(tx.src.hash, tx.dst.hash)) {
      memPool
        .update(tx.src.hash, { a =>
          a.copy(balance = a.balance - tx.amount)
        }, AddressCacheData(0L, 0L))
        .flatMap(
          _ =>
            memPool.update(tx.dst.hash, { a =>
              a.copy(balance = a.balance + tx.amount)
            }, AddressCacheData(tx.amount, 0L))
        )
    }

  def transferSnapshot(tx: Transaction): F[AddressCacheData] =
    memPool.update(tx.src.hash, { a =>
      a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - tx.amount)
    }, AddressCacheData(0L, 0L)) >>
      memPool.update(tx.dst.hash, { a =>
        a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + tx.amount)
      }, AddressCacheData(tx.amount, 0L))

  def lockForSnapshot(addresses: Set[Address], fn: F[Unit]): F[Unit] =
    locks.acquire(addresses.map(_.hash).toList) {
      fn
    }
}
