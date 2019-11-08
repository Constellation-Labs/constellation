package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.checkpoint.CheckpointService
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.primitives.{Schema, Transaction}

import scala.math.ceil

class RateLimiting[F[_]: Concurrent: Logger]() {
  private[storage] val counter: SingleRef[F, Map[Schema.Address, Int]] = SingleRef[F, Map[Schema.Address, Int]](Map())
  private[storage] val blacklisted: StorageService[F, Int] = new StorageService("rate_limiting_blacklist".some)

  def update(txs: List[Transaction]): F[Unit] =
    for {
      grouped <- txs.groupBy(_.src).mapValues(_.size).pure[F]

      _ <- counter.acquire
      _ <- counter.updateUnsafe(_ |+| grouped)

//      _ <- counter.getUnsafe.flatTap(c => Logger[F].debug(s"Update [rate-limiting]: $c"))
      _ <- blacklist()
      _ <- counter.release
    } yield ()

  def reset(cbHashes: List[String])(checkpointService: CheckpointService[F]): F[Unit] =
    for {
      _ <- counter.acquire
      cbs <- cbHashes.map(checkpointService.fullData).sequence[F, Option[Schema.CheckpointCache]].map(_.flatten)
      txs = cbs.flatMap(_.checkpointBlock.get.transactions.toList)
      grouped = txs.groupBy(_.src).mapValues(_.size)

      _ <- counter.setUnsafe(grouped)

//      _ <- counter.getUnsafe.flatTap(c => Logger[F].debug(s"Reset [rate-limiting]: $c"))
      _ <- counter.getUnsafe
        .flatMap(_.map(a => available(a._1)).toList.sequence)
//        .flatTap(c => Logger[F].debug(s"Available [rate-limiting]: $c"))
      _ <- blacklist()
      _ <- counter.release
    } yield ()

  def available(address: Schema.Address): F[Int] =
    for {
      c <- counter.getUnsafe
      accounts = c.size
      address <- counter.getUnsafe.map(_.getOrElse(address, 0))
      available = (if (accounts > 0) limits.available / accounts else limits.available) - address
    } yield available

  private def blacklist(): F[Unit] =
    for {
      c <- counter.getUnsafe
      left <- c.keys.toList.traverse(a => available(a).map(a -> _))
      _ <- left.filter(_._2 <= 0).traverse(p => blacklisted.update(p._1.address, _ => p._2, p._2))
    } yield ()

  object limits {
    val total = 50
    val offlineTxs = 0.01

    val offline: Int = ceil(total * offlineTxs).toInt
    val available: Int = total - offline
  }
}

object RateLimiting {
  def apply[F[_]: Concurrent: Logger]() = new RateLimiting[F]()
}
