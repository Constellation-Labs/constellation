package org.constellation.rewards

import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RewardsManagerTest
    extends FreeSpec
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  val snapshotHeightInterval = 2L

  "epoch 1" - {
    val expectedTotalPerSnapshot = 164.61
    val maxMintingValue = 853333333.20.round

    "reward per snapshot matches expected" in {
      val perSnapshotReward = BigDecimal(RewardsManager.totalRewardPerSnapshot(2))
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
      perSnapshotReward shouldBe expectedTotalPerSnapshot
    }

    "returns correct epoch for max height" in {
      val max = RewardsManager.epochOneMaxSnapshotHeight
      RewardsManager.snapshotEpoch(max) shouldBe 1
    }

    "returns next epoch for height greater than max" in {
      val max = RewardsManager.epochOneMaxSnapshotHeight + snapshotHeightInterval
      RewardsManager.snapshotEpoch(max) > 1 shouldBe true
    }

    "won't mint more than expected" in {
      val simulatedSnapshots =
        Range.Long(
          snapshotHeightInterval,
          RewardsManager.epochOneMaxSnapshotHeight + snapshotHeightInterval,
          snapshotHeightInterval
        )
      val totalRewardAfterSimulation =
        simulatedSnapshots.map(RewardsManager.totalRewardPerSnapshot).sum.round
      totalRewardAfterSimulation shouldBe maxMintingValue
    }
  }

  "epoch 2" - {
    "total epoch reward is half of previous epoch reward" in {
      RewardsManager.epochTwoRewards shouldBe RewardsManager.epochOneRewards / 2
    }

    "returns correct epoch for max height" in {
      val max = RewardsManager.epochTwoMaxSnapshotHeight
      RewardsManager.snapshotEpoch(max) shouldBe 2
    }

    "returns next epoch for height greater than max" in {
      val max = RewardsManager.epochTwoMaxSnapshotHeight + snapshotHeightInterval
      RewardsManager.snapshotEpoch(max) > 2 shouldBe true
    }
  }

  "epoch 3" - {
    "total epoch reward is half of previous epoch reward" in {
      RewardsManager.epochThreeRewards shouldBe RewardsManager.epochTwoRewards / 2
    }

    "returns correct epoch for max height" in {
      val max = RewardsManager.epochThreeMaxSnapshotHeight
      RewardsManager.snapshotEpoch(max) shouldBe 3
    }

    "returns next epoch for height greater than max" in {
      val max = RewardsManager.epochThreeMaxSnapshotHeight + snapshotHeightInterval
      RewardsManager.snapshotEpoch(max) > 3 shouldBe true
    }
  }

  "epoch 4" - {
    "total epoch reward is half of previous epoch reward" in {
      RewardsManager.epochFourRewards shouldBe RewardsManager.epochThreeRewards / 2
    }

    "returns correct epoch for max height" in {
      val max = RewardsManager.epochFourMaxSnapshotHeight
      RewardsManager.snapshotEpoch(max) shouldBe 4
    }

    "reward per snapshot is 0 for height out of bounds" in {
      val max = RewardsManager.epochFourMaxSnapshotHeight + snapshotHeightInterval
      RewardsManager.totalRewardPerSnapshot(RewardsManager.snapshotEpoch(max)) shouldBe 0
    }

    "returns -1 epoch for height out of bounds" in {
      val max = RewardsManager.epochFourMaxSnapshotHeight + snapshotHeightInterval
      RewardsManager.snapshotEpoch(max) shouldBe -1
    }
  }
}
