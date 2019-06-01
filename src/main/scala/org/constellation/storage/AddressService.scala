package org.constellation.storage

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.Transaction
import org.constellation.primitives.concurrency.MultiLock
import org.constellation.util.Metrics

class AddressService()(implicit metrics: () => Metrics) extends StorageService[IO, AddressCacheData]() {

  override def lookup(key: String): IO[Option[AddressCacheData]] = {
    super.lookup(key).flatTap { _ => IO {
      if (metrics() != null) {
        metrics().incrementMetric(s"address_query_$key")
      }
    }}
  }

  def transfer(tx: Transaction): IO[AddressCacheData] =
    AddressService.locks.acquire(List(tx.src.hash, tx.dst.hash)) {
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

  def transferSnapshot(tx: Transaction): IO[AddressCacheData] =
    update(tx.src.hash, { a =>
      a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - tx.amount)
    }, AddressCacheData(0L, 0L))
      .flatMap(
        _ =>
          update(tx.dst.hash, { a =>
            a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + tx.amount)
          }, AddressCacheData(tx.amount, 0L))
      )

  def lockForSnapshot(addresses: Set[Address], fn: IO[Unit]): IO[Unit] =
    AddressService.locks.acquire(addresses.map(_.hash).toList) {
      fn
    }
}

object AddressService {
  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  private val locks = new MultiLock[IO, String]()
}
