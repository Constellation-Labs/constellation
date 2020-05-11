package org.constellation.rewards

import better.files.File
import cats.implicits._
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.primitives.Schema
import org.constellation.serializer.KryoSerializer
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RewardsNormalizerTest
    extends FreeSpec
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  val numberOfNodes: Int = 12
  val snapshotsPerMinute: Int = 4

  val previousPerSnapshotWithStardust: Double = 444.44
  val previousPerSnapshot: Double = previousPerSnapshotWithStardust - (previousPerSnapshotWithStardust * 0.1)
  val previousPerNode: Double = previousPerSnapshot / numberOfNodes
  val previousPerStardust: Double = previousPerSnapshotWithStardust - previousPerSnapshot

  val expectedPerSnapshotWithStardust: Double = 164.61
  val expectedPerSnapshot: Double = expectedPerSnapshotWithStardust - (expectedPerSnapshotWithStardust * 0.1)
  val expectedPerNode: Double = expectedPerSnapshot / numberOfNodes

  val diff: Double = previousPerSnapshotWithStardust / expectedPerSnapshotWithStardust

  "normalizes rewards" in {
    val snapshotInfo = File(
      getClass.getResource("/rewards/d2e8654d8327c1ca4a97afda74084c0c4bcdf47ab2ad691f3ad39f10d1b2c83f-snapshot_info")
    )
    val deserializedSnapshotInfo = KryoSerializer.deserializeCast[SnapshotInfo](snapshotInfo.byteArray)

    println(
      s"Deserialized SnapshotInfo with hash ${deserializedSnapshotInfo.snapshot.snapshot.hash} and height ${deserializedSnapshotInfo.lastSnapshotHeight}"
    )

    val validatorRewardsAddress = "DAG4GcJgTED2RV1GUWAQVfwjpwaWicJBY7e3QmqP"

    val addressCacheData = deserializedSnapshotInfo.addressCacheData
      .filterKeys(_ != validatorRewardsAddress)

    // Normalization

    val rewardsBalances = addressCacheData
      .mapValues(_.rewardsBalance)

    val nonRewardsBalances = addressCacheData
      .mapValues(d => d.balanceByLatestSnapshot - d.rewardsBalance)
      .mapValues(_.toDouble)

    val fixedRewardsBalances = rewardsBalances
      .mapValues(b => b / diff)

    val fixedBalances = fixedRewardsBalances |+| nonRewardsBalances

    val csv = fixedBalances.toArray.map {
      case (address, balance) => s"$address,${balance.toLong}"
    }.mkString("\n")

    File("genesis.csv").createFileIfNotExists().clear().write(csv)

    val addressesWithRewards = rewardsBalances.filter { case (_, rewards) => rewards > 0 }
    println(s"\nFound ${addressesWithRewards.size} addresses with rewards (denormalized):")
    addressesWithRewards.foreach {
      case (address, rewards) => println(s"$address - ${rewards / Schema.NormalizationFactor}")
    }

    println(s"\nDividing by $diff ($previousPerSnapshotWithStardust / $expectedPerSnapshotWithStardust)")

    println("\nDivided reward balances (denormalized):")
    val fixedAddressesWithRewards = fixedRewardsBalances.filter { case (_, rewards) => rewards > 0 }
    fixedAddressesWithRewards.foreach {
      case (address, rewards) => println(s"$address - ${rewards / Schema.NormalizationFactor}")
    }

    println("\nTime since first reward till last reward:")
    addressesWithRewards.foreach {
      case (address, rewards) => {
        val divisor = if (address == StardustCollective.getAddress) previousPerStardust else previousPerNode

        val snapshots = (rewards / Schema.NormalizationFactor / divisor)
        val time = snapshots / snapshotsPerMinute
        val days = (time / 24 / 60).floor.toLong
        val hours = (time / 60 % 24).floor.toLong
        val minutes = (time % 60).floor.toLong
        val duration = s"$days days $hours hours $minutes minutes"
        println(s"$address - ${duration}")
      }
    }

    println("\nCSV:")
    println(csv)

  }

}
