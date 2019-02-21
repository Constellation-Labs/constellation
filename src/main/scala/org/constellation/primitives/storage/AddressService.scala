package org.constellation.primitives.storage

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.concurrency.MultiLock

class AddressService(size: Int) extends StorageService[AddressCacheData](size) {
  def transfer(src: Address, dst: Address, amount: Long): IO[AddressCacheData] =
    AddressService.locks.acquire(List(src.hash, dst.hash)) {
      updateAsync(src.hash, { a =>
        a.copy(balance = a.balance - amount)
      }, AddressCacheData(0L, 0L))
        .flatMap(
          _ =>
            updateAsync(dst.hash, { a =>
              a.copy(balance = a.balance + amount)
            }, AddressCacheData(amount, 0L))
        )
    }

  def transferSnapshot(src: Address, dst: Address, amount: Long): IO[AddressCacheData] =
    AddressService.locks.acquire(List(src.hash, dst.hash)) {
      updateAsync(src.hash, { a =>
        a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - amount)
      }, AddressCacheData(0L, 0L))
        .flatMap(
          _ =>
            updateAsync(dst.hash, { a =>
              a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + amount)
            }, AddressCacheData(amount, 0L))
        )
    }
}

object AddressService {
  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  private val locks = new MultiLock[IO, String]()
}
