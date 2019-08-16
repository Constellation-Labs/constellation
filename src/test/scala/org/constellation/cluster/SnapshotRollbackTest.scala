package org.constellation.cluster

import better.files.File
import cats.implicits._
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Transaction
import org.constellation.serializer.KryoSerializer
import org.scalatest.{FlatSpec, Ignore}

class SnapshotRollbackTest extends FlatSpec {

  private val snapshotsFolder = "rollback_data"
  private val snapshotInfoFile = "rollback_info"
  private val zeroSnapshotHash = Snapshot("", Seq()).hash

  type AccountBalances = Map[String, Long]

  "Rollback" should "validate snapshots from files" in {
    if (File("rollback_data").notExists) cancel

    val snapshots = loadSnapshotsFromFile()
    val snapshotInfo = loadSnapshotInfoFromFile()

    val newestSnapshot = findSnapshot(snapshotInfo.snapshot.lastSnapshot, snapshots)
    val accountBalances = calculateAccountBalances(newestSnapshot.snapshot.hash, snapshots)

    validateAccountBalance(accountBalances)
  }

  private def loadSnapshotsFromFile(): Seq[StoredSnapshot] =
    File(snapshotsFolder).list.toSeq
      .filter(_.name != snapshotInfoFile)
      .map(s => KryoSerializer.deserializeCast[StoredSnapshot](s.byteArray))

  private def loadSnapshotInfoFromFile(): SnapshotInfo =
    KryoSerializer.deserializeCast[SnapshotInfo](File(snapshotsFolder, snapshotInfoFile).byteArray)

  private def findSnapshot(hash: String, snapshots: Seq[StoredSnapshot]) =
    snapshots.find(_.snapshot.hash == hash) match {
      case Some(value) => value
      case None        => throw new Exception(s"Cannot find snapshot $hash")
    }

  private def calculateAccountBalances(snapshotHash: String, snapshots: Seq[StoredSnapshot]): AccountBalances =
    if (snapshotHash == zeroSnapshotHash) {
      Map.empty[String, Long]
    } else {
      val snapshot = findSnapshot(snapshotHash, snapshots)
      getAccountBalancesFrom(snapshot) |+| calculateAccountBalances(snapshot.snapshot.lastSnapshot, snapshots)
    }

  private def getAccountBalancesFrom(snapshot: StoredSnapshot): AccountBalances = {
    val transactions: Seq[Transaction] = snapshot.checkpointCache
      .flatMap(_.checkpointBlock)
      .flatMap(_.transactions)

    val spend: Map[String, Long] = transactions
      .groupBy(_.src.address)
      .mapValues(-_.map(_.amount).sum)

    val received: Map[String, Long] = transactions
      .groupBy(_.dst.address)
      .mapValues(_.map(_.amount).sum)

    spend |+| received
  }

  private def validateAccountBalance(accountBalance: AccountBalances): Unit =
    accountBalance.filter(_._2 < 0).foreach(a => throw new Exception(s"Account balance is lower than zero : $a"))
}
