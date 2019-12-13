package org.constellation.storage

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointService
import org.constellation.primitives.{Schema, Transaction}

import scala.math.ceil

class RateLimiting[F[_]: Concurrent]() {
  val logger = Slf4jLogger.getLogger[F]

  private[storage] val counter: Ref[F, Map[Schema.Address, Int]] = Ref.unsafe[F, Map[Schema.Address, Int]](Map())
  private[storage] val blacklisted: StorageService[F, Int] = new StorageService("rate_limiting_blacklist".some)

  def update(txs: List[Transaction]): F[Unit] = Concurrent[F].unit
  def reset(cbHashes: List[String])(checkpointService: CheckpointService[F]): F[Unit] = Concurrent[F].unit
  def available(address: Schema.Address): F[Int] = Concurrent[F].pure(limits.available)
  def blacklist(): F[Unit] = Concurrent[F].unit

  /*
  def update(txs: List[Transaction]): F[Unit] =
    for {
      grouped <- txs.groupBy(_.src).mapValues(_.size).pure[F]
      _ <- counter.modify(c => (c |+| grouped, ()))

//      _ <- counter.get.flatTap(c => logger.debug(s"Update [rate-limiting]: $c"))
      _ <- blacklist()
    } yield ()

  def reset(cbHashes: List[String])(checkpointService: CheckpointService[F]): F[Unit] =
    for {
      cbs <- cbHashes.map(checkpointService.fullData).sequence[F, Option[Schema.CheckpointCache]].map(_.flatten)
      txs = cbs.flatMap(_.checkpointBlock.transactions.toList)
      grouped = txs.groupBy(_.src).mapValues(_.size)

      _ <- counter.modify(_ => (grouped, ()))

//      _ <- counter.get.flatTap(c => logger.debug(s"Reset [rate-limiting]: $c"))
      _ <- counter.get
        .flatMap(_.map(a => available(a._1)).toList.sequence)
//        .flatTap(c => logger.debug(s"Available [rate-limiting]: $c"))
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

   */

  object limits {
    val total = 50
    val offlineTxs = 0.01

    val offline: Int = ceil(total * offlineTxs).toInt
    val available: Int = total - offline
  }
}

object RateLimiting {
  def apply[F[_]: Concurrent]() = new RateLimiting[F]()
}
