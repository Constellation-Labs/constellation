package org.constellation.storage

import cats.effect.Concurrent
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.schema.address.AddressCacheData
import org.constellation.primitives.concurrency.MultiLock
import org.constellation.schema.transaction.Transaction

class AddressService[F[_]: Concurrent]() {

  private val logger = Slf4jLogger.getLogger[F]

  private val locks = new MultiLock[F, String]()

  private val memPool = new StorageService[F, AddressCacheData]("address_mem_pool".some, None)

  def getAll: F[Map[String, AddressCacheData]] = memPool.toMap()

  def setAll(kv: Map[String, AddressCacheData]): F[Map[String, AddressCacheData]] =
    getAll.flatMap { pool =>
      withLock(kv.keySet ++ pool.keySet) {
        clear >> memPool.putAll(kv)
      }
    }

  def set(key: String, value: AddressCacheData): F[AddressCacheData] = withLock(Set(key)) {
    memPool.put(key, value)
  }

  def size: F[Long] = memPool.size()

  def lookup(key: String): F[Option[AddressCacheData]] =
    memPool.lookup(key)

  def clear: F[Unit] =
    memPool.clear
      .flatTap(_ => logger.info("AddressService has been cleared"))

  def transferTransaction(tx: Transaction): F[AddressCacheData] =
    withLock(Set(tx.src.hash, tx.dst.hash)) {
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
    withLock(rewardsDistribution.keySet) {
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

  def transferSnapshotTransaction(tx: Transaction): F[AddressCacheData] =
    withLock(Set(tx.src.hash, tx.dst.hash)) {
      memPool.update(tx.src.hash, { a =>
        a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - tx.amount - tx.feeValue)
      }, AddressCacheData(0L, 0L)) >>
        memPool.update(tx.dst.hash, { a =>
          a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + tx.amount)
        }, AddressCacheData(tx.amount, 0L))
    }

  private[storage] def withLock[R](addresses: Set[String])(thunk: => F[R]): F[R] =
    locks.acquire(addresses.toList) {
      thunk
    }
}
