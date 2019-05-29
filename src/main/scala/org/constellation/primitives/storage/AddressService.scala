package org.constellation.primitives.storage

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.Transaction
import org.constellation.primitives.concurrency.MultiLock
import org.constellation.util.Metrics

class AddressService(size: Int)(implicit metrics: Unit => Metrics) extends StorageService[AddressCacheData](size) {

  override def getSync(key: String): Option[AddressCacheData] = {
    if (metrics() != null) {
      metrics().incrementMetric(s"address_query_$key")
    }

    super.getSync(key)
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
