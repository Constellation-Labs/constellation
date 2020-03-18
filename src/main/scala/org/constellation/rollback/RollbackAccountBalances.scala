package org.constellation.rollback

import cats.implicits._
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.util.AccountBalances
import org.constellation.util.AccountBalances.AccountBalances

import scala.annotation.tailrec
import scala.util.Try

class RollbackAccountBalances {

  private val zeroSnapshotHash = Snapshot.snapshotZeroHash

  private def calculateSnapshotBalances(snapshot: StoredSnapshot): AccountBalances =
    AccountBalances.getAccountBalancesFrom(snapshot)

  private def calculateSnapshotInfoBalances(snapshotInfo: SnapshotInfo): AccountBalances =
    AccountBalances.getAccountBalancesFrom(snapshotInfo.snapshot)

  def calculate(genesisObservation: GenesisObservation): Either[RollbackException, AccountBalances] =
    Try(calculateGenesisObservationBalances(genesisObservation))
      .map(Right(_))
      .getOrElse(Left(CannotCalculate))

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
