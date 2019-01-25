package org.constellation.primitives.storage

import cats.effect.{ContextShift, IO}
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.concurrency.MultiLock

class AddressService(size: Int) extends StorageService[AddressCacheData](size) {
  def transfer(src: Address, dst: Address, amount: Long): IO[AddressCacheData] = {
    AddressService.locks.acquire(List(src.hash, dst.hash)) {
      IO {
        update(
          src.hash,
          { a: AddressCacheData => a.copy(balance = a.balance - amount) },
          AddressCacheData(0L, 0L))
        update(
          dst.hash,
          { a: AddressCacheData => a.copy(balance = a.balance + amount) },
          AddressCacheData(amount, 0L))
      }
    }
  }
}

object AddressService {
  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  private val locks = new MultiLock[IO, String]()
}
