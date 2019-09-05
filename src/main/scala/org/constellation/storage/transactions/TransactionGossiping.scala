package org.constellation.storage.transactions

import cats.effect.{Clock, Concurrent, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.{Logger, SelfAwareStructuredLogger}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.primitives.Schema.Id
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.{ConsensusStatus, TransactionService}
import org.constellation.util.Logging._

import scala.util.Random

class TransactionGossiping[F[_]: Concurrent: Logger: Clock](
  transactionService: TransactionService[F],
  fanout: Int,
  dao: DAO
) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def selectPeers(tx: TransactionCacheData)(implicit random: Random): F[Set[Id]] =
    for {
      peers <- getDiffPeers(tx)
      randomPeers <- Sync[F].delay(random.shuffle(peers))
    } yield randomPeers.take(fanout)

  private def getDiffPeers(tx: TransactionCacheData): F[Set[Id]] =
    for {
      all <- LiftIO[F].liftIO(dao.peerInfo)
      used <- getUsedPeers(tx)
    } yield all.keySet.diff(used)

  private def getUsedPeers(tx: TransactionCacheData): F[Set[Id]] =
    tx.path.pure[F]

  def observe(tx: TransactionCacheData): F[TransactionCacheData] =
    logThread(
      for {
        _ <- logger.debug(s"Observing transaction=${tx.hash}")
        updated <- transactionService.update(
          tx.transaction.hash,
          t => t.copy(path = t.path ++ tx.path),
          tx.copy(path = tx.path + dao.id),
          ConsensusStatus.Unknown
        )
      } yield updated,
      "transactionGossiping_observe"
    )

}
