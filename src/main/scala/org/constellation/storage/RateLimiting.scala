package org.constellation.storage

import cats.effect.concurrent.Ref
import cats.effect.Sync
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.primitives.{Schema, Transaction}

import scala.math.ceil

class RateLimiting[F[_]: Sync: Logger]() {
  private[storage] val counter: Ref[F, Map[Schema.Address, Int]] = Ref.unsafe(Map())
  private[storage] val blacklisted: StorageService[F, Int] = new StorageService()

  def update(txs: List[Transaction]): F[Unit] =
    for {
      grouped <- txs.groupBy(_.src).mapValues(_.size).pure[F]
      _ <- counter.update(_ |+| grouped)

      _ <- counter.get.flatTap(c => Logger[F].debug(s"Update [rate-limiting]: $c"))
      _ <- blacklist()
    } yield ()

  def reset(cbHashes: List[String])(checkpointService: CheckpointService[F]): F[Unit] =
    for {
      cbs <- cbHashes.traverse(checkpointService.fullData).map(_.flatten)
      txs = cbs.flatMap(_.checkpointBlock.get.transactions.toList)
      grouped = txs.groupBy(_.src).mapValues(_.size)
      _ <- counter.set(grouped)

      _ <- counter.get.flatTap(c => Logger[F].debug(s"Reset [rate-limiting]: $c"))
      _ <- counter.get
        .flatMap(_.map(a => available(a._1)).toList.sequence)
        .flatTap(c => Logger[F].info(s"Available [rate-limiting]: $c"))
      _ <- blacklist()
    } yield ()

  def available(address: Schema.Address): F[Int] =
    for {
      c <- counter.get
      accounts = c.size
      address <- counter.get.map(_.getOrElse(address, 0))
      available = (if (accounts > 0) limits.available / accounts else limits.available) - address
    } yield available

  private def blacklist(): F[Unit] =
    for {
      c <- counter.get
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
  def apply[F[_]: Sync: Logger]() = new RateLimiting[F]()
}
