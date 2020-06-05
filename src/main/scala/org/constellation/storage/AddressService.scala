package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.primitives.Schema.{Address, AddressCacheData}
import org.constellation.primitives.Transaction
import org.constellation.primitives.concurrency.MultiLock
import org.constellation.schema.Id

class AddressService[F[_]: Concurrent]() {

  private val logger = Slf4jLogger.getLogger[F]

  private val locks = new MultiLock[F, String]()

  private val memPool = new StorageService[F, AddressCacheData]("address_mem_pool".some, None)

  def toMap: F[Map[String, AddressCacheData]] = memPool.toMap()

  def putUnsafe(key: String, value: AddressCacheData): F[AddressCacheData] = memPool.put(key, value)

  def size: F[Long] = memPool.size()

  def lookup(key: String): F[Option[AddressCacheData]] =
    memPool.lookup(key)

  def transfer(tx: Transaction): F[AddressCacheData] =
    locks.acquire(List(tx.src.hash, tx.dst.hash)) {
      memPool
        .update(tx.src.hash, { a =>
          a.copy(balance = a.balance - tx.amount - tx.feeValue)
        }, AddressCacheData(0L, 0L))
        .flatMap(
          _ =>
            memPool.update(tx.dst.hash, { a =>
              a.copy(balance = a.balance + tx.amount)
            }, AddressCacheData(tx.amount, 0L))
        )
    }

  def transferRewards(rewardsDistribution: Map[String, Long]): F[Map[String, Long]] =
    locks.acquire(rewardsDistribution.keys.toList) {
      rewardsDistribution.toList.traverse {
        case (address, reward) =>
          memPool
            .update(
              address,
              a =>
                a.copy(
                  balance = a.balance + reward,
                  balanceByLatestSnapshot = a.balanceByLatestSnapshot + reward,
                  rewardsBalance = a.rewardsBalance + reward
                ),
              AddressCacheData(reward, reward)
            )
            .map(a => address -> a.rewardsBalance)
      }.map(_.toMap)
    }

  def transferSnapshot(tx: Transaction): F[AddressCacheData] =
    memPool.update(tx.src.hash, { a =>
      a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - tx.amount - tx.feeValue)
    }, AddressCacheData(0L, 0L)) >>
      memPool.update(tx.dst.hash, { a =>
        a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + tx.amount)
      }, AddressCacheData(tx.amount, 0L))

  def lockForSnapshot(addresses: Set[Address], fn: F[Unit]): F[Unit] =
    locks.acquire(addresses.map(_.hash).toList) {
      fn
    }

  def clear: F[Unit] =
    memPool.clear
      .flatTap(_ => logger.info("Address MemPool has been cleared"))
}
