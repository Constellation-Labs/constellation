package org.constellation.rollback

import cats.effect.Concurrent
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.util.AccountBalances
import org.constellation.util.AccountBalances.AccountBalances
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class RollbackAccountBalances[F[_]: Concurrent] {

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  private val zeroSnapshotHash = Snapshot("", Seq()).hash

  def calculate(snapshotHash: String, snapshots: Seq[StoredSnapshot]): Either[RollbackException, AccountBalances] =
    Try {
      calculateSnapshotsBalances(snapshotHash, snapshots)
    } match {
      case Success(value) => Right(value)
      case Failure(exception) =>
        logger.error(s"Cannot calculate account balances from snapshots ${exception.getMessage}")
        Left(CannotCalculate)
    }

  @tailrec
  private def calculateSnapshotsBalances(
    snapshotHash: String,
    snapshots: Seq[StoredSnapshot],
    balances: AccountBalances = Map.empty[String, Long]
  ): AccountBalances =
    if (snapshotHash == zeroSnapshotHash) {
      balances
    } else {
      val snapshot = findSnapshot(snapshotHash, snapshots)
      calculateSnapshotsBalances(
        snapshot.snapshot.lastSnapshot,
        snapshots,
        balances |+| AccountBalances.getAccountBalancesFrom(snapshot)
      )
    }

  private def findSnapshot(hash: String, snapshots: Seq[StoredSnapshot]) =
    snapshots.find(_.snapshot.hash == hash) match {
      case Some(value) => value
      case None =>
        logger.error(s"Cannot find snapshot $hash")
        throw new Exception(s"Cannot find snapshot $hash")
    }

  def calculate(genesisObservation: GenesisObservation): Either[RollbackException, AccountBalances] =
    Try {
      calculateGenesisObservationBalances(genesisObservation)
    } match {
      case Success(value) => Right(value)
      case Failure(exception) =>
        logger.error(s"Cannot calculate account balances from genesis observation ${exception.getMessage}")
        Left(CannotCalculate)
    }

  private def calculateGenesisObservationBalances(genesisObservation: GenesisObservation): AccountBalances = {
    val genesisBalances =
      AccountBalances.getGenesisAccountBalancesFrom(genesisObservation.genesis)
    val firstDistributionBalances =
      AccountBalances.getAccountBalancesFrom(genesisObservation.initialDistribution)
    val secondDistributionBalances =
      AccountBalances.getAccountBalancesFrom(genesisObservation.initialDistribution2)

    genesisBalances |+| firstDistributionBalances |+| secondDistributionBalances
  }
}
