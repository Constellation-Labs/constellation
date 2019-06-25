package org.constellation.storage.transactions

import cats.effect.{LiftIO, Sync}
import cats.implicits._
import org.constellation.DAO
import org.constellation.primitives.Schema.Id
import org.constellation.primitives.TransactionCacheData
import org.constellation.storage.TransactionService

import scala.util.Random

class TransactionGossiping[F[_]: Sync: LiftIO](transactionService: TransactionService[F], dao: DAO) {

  def selectPeers(tx: TransactionCacheData, fanout: Int)(implicit random: Random): F[Set[Id]] =
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

  def observe(tx: TransactionCacheData): F[Unit] =
    for {
      isKnown <- transactionService.contains(tx.transaction.hash)
      _ <- if (isKnown) Sync[F].unit else updateTxPath(tx)
    } yield ()

  private def updateTxPath(tx: TransactionCacheData): F[Unit] =
    transactionService.update(tx.transaction.hash, t => t.copy(path = t.path))

}
