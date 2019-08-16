package org.constellation.rollback

import cats.data.EitherT
import cats.effect.Concurrent
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.constellation.util.AccountBalances.AccountBalances
import org.slf4j.LoggerFactory

class RollbackService[F[_]: Concurrent](
  rollbackBalances: RollbackAccountBalances[F],
  rollbackLoader: RollbackLoader[F]
) {

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def rollback(): EitherT[F, RollbackException, Unit] =
    for {
      snapshots <- rollbackLoader.loadSnapshotsFromFile()
      snapshotInfo <- rollbackLoader.loadSnapshotInfoFromFile()
      genesisObservation <- rollbackLoader.loadGenesisObservation()

      balances <- EitherT.fromEither[F](rollbackBalances.calculate(snapshotInfo.snapshot.lastSnapshot, snapshots))
      genesisBalances <- EitherT.fromEither[F](rollbackBalances.calculate(genesisObservation))

      _ <- EitherT.fromEither[F](validateAccountBalance(balances |+| genesisBalances))
    } yield ()

  private def validateAccountBalance(accountBalances: AccountBalances): Either[RollbackException, Unit] =
    accountBalances.count(_._2 < 0) match {
      case 0 =>
        logger.info("Valid account balnces from rollback data")
        Right()
      case _ =>
        logger.error("Invalid account balances from rollback data")
        Left(InvalidBalances)
    }
}
