package org.constellation.storage

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.Transaction
import org.constellation.primitives.concurrency.MultiLock
import org.constellation.util.Metrics

class AddressService[F[_]: Concurrent]()(implicit metrics: () => Metrics)
    extends StorageService[F, AddressCacheData]() {
  private val locks = new MultiLock[F, String]()

  override def lookup(key: String): F[Option[AddressCacheData]] =
    super.lookup(key)
      .flatTap(_ => Sync[F].delay({
        if (metrics() != null) {
          metrics().incrementMetric(s"address_query_$key")
        }
      }))

  def transfer(tx: Transaction): F[AddressCacheData] =
    locks.acquire(List(tx.src.hash, tx.dst.hash)) {
      update(tx.src.hash, { a =>
        a.copy(balance = a.balance - tx.amount)
      }, AddressCacheData(0L, 0L))
        .flatMap(
          _ =>
            update(tx.dst.hash, { a =>
              a.copy(balance = a.balance + tx.amount)
            }, AddressCacheData(tx.amount, 0L))
        )
    }

  def transferSnapshot(tx: Transaction): F[AddressCacheData] =
    update(tx.src.hash, { a =>
      a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - tx.amount)
    }, AddressCacheData(0L, 0L))
      .flatMap(
        _ =>
          update(tx.dst.hash, { a =>
            a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + tx.amount)
          }, AddressCacheData(tx.amount, 0L))
      )

  def lockForSnapshot(addresses: Set[Address], fn: F[Unit]): F[Unit] =
    locks.acquire(addresses.map(_.hash).toList) {
      fn
    }
}
