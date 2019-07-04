package org.constellation.storage

import cats.effect.concurrent.Ref
import cats.effect.Sync
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.primitives.{Schema, Transaction}

import scala.math.ceil

class RateLimiting[F[_]: Sync: Logger]() {
  val counter: Ref[F, Map[Schema.Address, Int]] = Ref.unsafe(Map())

  def update(txs: List[Transaction]): F[Unit] =
    for {
      grouped <- txs.groupBy(_.src).mapValues(_.size).pure[F]
      _ <- counter.update(_ |+| grouped)
      _ <- counter.get.flatTap(c => Logger[F].info(s"Update: $c"))
    } yield ()

  def reset(cbHashes: List[String])(checkpointService: CheckpointService[F]): F[Unit] =
    for {
      cbs <- cbHashes.traverse(checkpointService.fullData).map(_.flatten)
      txs = cbs.flatMap(_.checkpointBlock.get.transactions.toList)
      grouped = txs.groupBy(_.src).mapValues(_.size)
      _ <- counter.set(grouped)
      _ <- counter.get.flatTap(c => Logger[F].info(s"Reset: $c"))
      _ <- counter.get
        .flatMap(_.map(a => available(a._1)).toList.sequence)
        .flatTap(c => Logger[F].info(s"Available: $c"))
    } yield ()

  def available(address: Schema.Address): F[Int] =
    for {
      c <- counter.get
      accounts = c.size
      address <- counter.get.map(_.getOrElse(address, 0))
      available = (limits.available / accounts) - address
    } yield available

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
