package org.constellation.rollback

import cats.implicits._
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.util.AccountBalances
import org.constellation.util.AccountBalances.AccountBalances

import scala.annotation.tailrec
import scala.util.Try

class RollbackAccountBalances {

  private val zeroSnapshotHash = Snapshot("", Seq(), Map.empty).hash

  def calculate(snapshotHash: String, snapshots: Seq[StoredSnapshot]): Either[RollbackException, AccountBalances] =
    Try(calculateSnapshotsBalances(snapshotHash, snapshots))
      .map(Right(_))
      .getOrElse(Left(CannotCalculate))

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
      case None        => throw new Exception(s"Cannot find snapshot $hash")
    }

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
