package org.constellation.rewards

import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.checkpoint.CheckpointService
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.p2p.{Cluster, MajorityHeight}
import org.constellation.schema.address.Address
import org.constellation.schema.checkpoint.CheckpointEdge
import org.constellation.schema.observation.Observation
import org.constellation.schema.snapshot.Snapshot
import org.constellation.schema.transaction.Transaction
import org.constellation.schema.{ChannelMessage, Id, PeerNotification, Schema}
import org.constellation.storage.AddressService
import org.constellation.trust.TrustEdge
import org.constellation.util.Metrics

case class RewardSnapshot(
  hash: String,
  height: Long,
  observations: Seq[Observation]
)

class RewardsManager[F[_]: Concurrent](
  eigenTrust: EigenTrust[F],
  addressService: AddressService[F],
  selfAddress: String,
  metrics: Metrics,
  clusterStorage: ClusterStorageAlgebra[F],
  nodeStorage: NodeStorageAlgebra[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  nodeId: Id
) {
  private val logger = Slf4jLogger.getLogger[F]

  private[rewards] var lastRewardedHeight: Ref[F, Long] = Ref.unsafe(-1L)

  def setLastRewardedHeight(height: Long): F[Unit] = lastRewardedHeight.modify(_ => (height, ()))

  def getLastRewardedHeight(): F[Long] = lastRewardedHeight.get

  def clearLastRewardedHeight(): F[Unit] = setLastRewardedHeight(-1)

  def attemptReward(snapshot: Snapshot, height: Long): F[Unit] =
    createRewardSnapshot(snapshot, height)
      .flatMap(updateBySnapshot)

  private[rewards] def createRewardSnapshot(snapshot: Snapshot, height: Long): F[RewardSnapshot] =
    observationsFromSnapshot(snapshot).map(RewardSnapshot(snapshot.hash, height, _))

  private[rewards] def updateBySnapshot(rewardSnapshot: RewardSnapshot): F[Unit] =
    for {
      _ <- logger.debug(
        s"Updating rewards by snapshot ${rewardSnapshot.hash} at height ${rewardSnapshot.height}"
      )
      _ <- Concurrent[F]
        .pure(rewardSnapshot.observations.isEmpty)
        .ifM(
          logger.debug(s"${rewardSnapshot.hash} has no observations"),
          logger.debug(s"${rewardSnapshot.hash} contains following observations:").flatMap { _ =>
            rewardSnapshot.observations.toList
              .map(_.signedObservationData.data)
              .traverse(o => logger.debug(s"Observation for ${o.id}, ${o.event}"))
              .void
          }
        )
      _ <- eigenTrust.retrain(rewardSnapshot.observations)
      trustMap <- eigenTrust.getTrustForAddresses

      peers <- clusterStorage.getPeers

      ownJoiningHeight <- nodeStorage.getOwnJoinedHeight
      ownMajorityHeight = MajorityHeight(
        joined = ownJoiningHeight
      )

      peersMajorityHeightsWithOwn = peers
        .mapValues(_.majorityHeight) + (nodeId -> NonEmptyList.of(ownMajorityHeight))

      _ <- logger.debug("Majority heights for rewarding:")
      _ <- peersMajorityHeightsWithOwn.toList.traverse {
        case (id, majorityHeights) =>
          for {
            _ <- logger.debug(s"${id.address}: ${majorityHeights}")
          } yield ()
      }

      nodesWithProposalsOnly = peersMajorityHeightsWithOwn.filter {
        case (_, majorityHeight) => MajorityHeight.isHeightBetween(rewardSnapshot.height)(majorityHeight)
      }.keySet.map(_.address)

      weightContributions = weightByTrust(trustMap) _ >>>
        weightByEpoch(rewardSnapshot.height) >>>
        weightByDTM >>>
        weightBySoftStaking(rewardSnapshot) >>>
        weightByStardust
      contributions = calculateContributions(nodesWithProposalsOnly)
      distribution = weightContributions(contributions)
      normalizedDistribution = normalize(distribution)

      _ <- logger.debug(s"Distribution (${rewardSnapshot.hash}):")
      _ <- normalizedDistribution.toList.traverse {
        case (address, reward) => logger.debug(s"Address: ${address}, Reward: ${reward}")
      }

      rewardBalances <- updateAddressBalances(normalizedDistribution)
      _ <- lastRewardedHeight.modify(_ => (rewardSnapshot.height, ()))

      _ <- metrics.updateMetricAsync("rewards_selfBalanceAfterReward", rewardBalances.getOrElse(selfAddress, 0L))
      _ <- metrics.updateMetricAsync(
        "rewards_stardustBalanceAfterReward",
        rewardBalances.getOrElse(StardustCollective.getAddress(), 0L)
      )

      _ <- metrics.updateMetricAsync("rewards_selfSnapshotReward", normalizedDistribution.getOrElse(selfAddress, 0L))
      _ <- metrics.updateMetricAsync(
        "rewards_stardustSnapshotReward",
        normalizedDistribution.getOrElse(StardustCollective.getAddress(), 0L)
      )

      _ <- metrics
        .updateMetricAsync("rewards_snapshotReward", normalizedDistribution.map { case (_, value) => value }.sum)
      _ <- metrics
        .updateMetricAsync(
          "rewards_snapshotRewardWithoutStardust",
          normalizedDistribution
            .filterKeys(_ != StardustCollective.getAddress())
            .map { case (_, value) => value }
            .sum
        )
      _ <- metrics.incrementMetricAsync("rewards_snapshotCount")
      _ <- metrics.updateMetricAsync("rewards_lastRewardedHeight", rewardSnapshot.height)

      _ <- logger.debug(
        s"Rewarding by ${rewardSnapshot.hash} succeeded."
      )
    } yield ()

  private def normalize(contributions: Map[String, Double]): Map[String, Long] =
    contributions
      .mapValues(Schema.NormalizationFactor * _)
      .mapValues(_.toLong)

  def weightByStardust: Map[String, Double] => Map[String, Double] =
    StardustCollective.weightByStardust(Set(DTM.getAddress))

  def weightByDTM: Map[String, Double] => Map[String, Double] =
    DTM.weightByDTM

  def weightBySoftStaking(rewardSnapshot: RewardSnapshot): Map[String, Double] => Map[String, Double] = {
    val ignore = Set(DTM.getAddress)
    rewardSnapshot.height match {
      case height if height >= 3914020 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 4789) // September
      case height if height >= 3826200 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5602) // August
      case height if height >= 3738250 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5277) // July
      case height if height >= 3649202 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5628) // June
      case height if height >= 3567568 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5712) // May
      case height if height >= 3484394 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5451) // April
      case height if height >= 3398732 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5424) // March
      case height if height >= 3319758 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 5552) // February
      case height if height >= 3232768 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 4119) // January
      case height if height >= 3146548 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 3778) // December
      case height if height >= 3076775 => SoftStaking.weightBySoftStaking(ignore)(softStakingNodes = 3570) // November
      case _                           => identity
    }
  }

  private def weightByEpoch(snapshotHeight: Long)(contributions: Map[String, Double]): Map[String, Double] =
    contributions.mapValues(_ * RewardsManager.totalRewardPerSnapshot(snapshotHeight))

  private def weightByTrust(
    trustEntropyMap: Map[String, Double]
  )(contributions: Map[String, Double]): Map[String, Double] = {
    val weightedEntropy = contributions.transform {
      case (address, partitionSize) =>
        partitionSize * (1 - trustEntropyMap(address)) // Normalize wrt total partition space
    }
    val totalEntropy = weightedEntropy.values.sum
    weightedEntropy.mapValues(_ / totalEntropy) // Scale by entropy magnitude
  }

  /**
    * Calculates non-weighted contributions so each address takes equal part which is calculated
    * as 1/totalSpace
    *
    * @param ids
    * @return
    */
  private def calculateContributions(ids: Set[String]): Map[String, Double] = {
    val totalSpace = ids.size
    val contribution = 1.0 / totalSpace
    ids.map { id =>
      id -> contribution
    }.toMap
  }

  private def observationsFromSnapshot(snapshot: Snapshot): F[Seq[Observation]] =
    snapshot.checkpointBlocks.toList
      .traverse(checkpointStorage.getCheckpoint)
      .map(_.flatten.flatMap(_.checkpointBlock.observations))

  private def updateAddressBalances(rewards: Map[String, Long]): F[Map[String, Long]] =
    addressService.transferRewards(rewards)
}

object RewardsManager {
  val roundingError = 0.000000000001

  val snapshotsPerMinute = 2.0
  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  val epochOneRewards = 853333333.20 // Normalized
  val epochTwoRewards: Double = epochOneRewards / 2
  val epochThreeRewards: Double = epochTwoRewards / 2
  val epochFourRewards: Double = epochThreeRewards / 2

  val epochOneDuration: Double = 2.5
  val epochTwoDuration: Double = 2.5
  val epochThreeDuration: Double = 2.5
  val epochFourDuration: Double = 2.5

  val epochOneMaxSnapshotHeight: Long = epochSnapshotHeightInterval(epochOneDuration)
  val epochTwoMaxSnapshotHeight: Long = epochOneMaxSnapshotHeight + epochSnapshotHeightInterval(epochTwoDuration)
  val epochThreeMaxSnapshotHeight: Long = epochTwoMaxSnapshotHeight + epochSnapshotHeightInterval(epochThreeDuration)
  val epochFourMaxSnapshotHeight: Long = epochThreeMaxSnapshotHeight + epochSnapshotHeightInterval(epochFourDuration)

  val epochOneRewardPerSnapshot: Double = epochRewardPerSnapshot(epochOneRewards)
  val epochTwoRewardsPerSnapshot: Double = epochRewardPerSnapshot(epochTwoRewards)
  val epochThreeRewardsPerSnapshot: Double = epochRewardPerSnapshot(epochThreeRewards)
  val epochFourRewardsPerSnapshot: Double = epochRewardPerSnapshot(epochFourRewards)

  val rewardsPool: Double = epochFourDuration

  //  /*
  //  Partitioning of address space, light nodes have smaller basis that full.
  //  Normalizes rewards based on node size.
  //   */
  //  val partitonChart: Map[String, Set[String]] = Map[String, Set[String]]()
  //
  //  /*
  //  Should come from reputation service.
  //   */
  //  val transitiveReputationMatrix: Map[String, Map[String, Double]] = Map[String, Map[String, Double]]()
  //  val neighborhoodReputationMatrix: Map[String, Double] = Map[String, Double]()

  def totalRewardPerSnapshot(snapshotHeight: Long): Double =
    snapshotEpoch(snapshotHeight) match {
      case 1 => epochOneRewardPerSnapshot
      case 2 => epochTwoRewardsPerSnapshot
      case 3 => epochThreeRewardsPerSnapshot
      case 4 => epochFourRewardsPerSnapshot
      case _ => 0d
    }

  private[rewards] def epochRewardPerSnapshot(epochTotalRewards: Double): Double =
    // It is based on validation sheet
    epochTotalRewards / 5 / 6 / 30 / 24 / 60 / snapshotsPerMinute

  private[rewards] def epochMaxSnapshotNumber(
    yearsDuration: Double,
    snapshotsPerMinute: Double = RewardsManager.snapshotsPerMinute
  ): Long = {
    val months = yearsDuration * 12
    val days = months * 30
    val hours = days * 24
    val minutes = hours * 60
    (minutes * snapshotsPerMinute).toLong
  }

  private[rewards] def epochSnapshotHeightInterval(
    yearsDuration: Double,
    snapshotsPerMinute: Double = RewardsManager.snapshotsPerMinute,
    snapshotHeightInterval: Long = snapshotHeightInterval
  ): Long = {
    val snapshotsPerEpoch = epochMaxSnapshotNumber(yearsDuration, snapshotsPerMinute)
    snapshotsPerEpoch * snapshotHeightInterval
  }

  private[rewards] def snapshotEpoch(snapshotHeight: Long): Int =
    snapshotHeight match {
      case num if num >= 0 && num <= epochOneMaxSnapshotHeight                           => 1
      case num if num > epochOneMaxSnapshotHeight && num <= epochTwoMaxSnapshotHeight    => 2
      case num if num > epochTwoMaxSnapshotHeight && num <= epochThreeMaxSnapshotHeight  => 3
      case num if num > epochThreeMaxSnapshotHeight && num <= epochFourMaxSnapshotHeight => 4
      case _                                                                             => -1
    }

  @deprecated("Old implementation for light nodes", "Since we have full-nodes only")
  def validatorRewards(
    curShapshot: Int,
    transitiveReputationMatrix: Map[String, Map[String, Double]],
    neighborhoodReputationMatrix: Map[String, Double],
    partitonChart: Map[String, Set[String]]
  ): Map[String, Double] = {
    val trustEntropyMap = shannonEntropy(transitiveReputationMatrix, neighborhoodReputationMatrix)
    val distro = rewardDistribution(partitonChart, trustEntropyMap)
    distro.mapValues(_ * totalRewardPerSnapshot(curShapshot))
  }

  @deprecated("Old implementation for light nodes", "Since we have full-nodes only")
  def shannonEntropy(
    transitiveReputationMatrix: Map[String, Map[String, Double]],
    neighborhoodReputationMatrix: Map[String, Double]
  ): Map[String, Double] = {
    val weightedTransitiveReputation = transitiveReputationMatrix.transform {
      case (key, view) => view.map { case (neighbor, score) => neighborhoodReputationMatrix(key) * score }.sum
    }
    weightedTransitiveReputation.mapValues { trust =>
      if (trust == 0.0) 0.0
      else -trust * math.log(trust) / math.log(2)
    }
  }

  @deprecated("Old implementation for light nodes", "Since we have full-nodes only")
  def rewardDistribution(
    partitonChart: Map[String, Set[String]],
    trustEntropyMap: Map[String, Double]
  ): Map[String, Double] = {
    val totalSpace = partitonChart.values.map(_.size).max
    val contributions = partitonChart.mapValues(partiton => (partiton.size / totalSpace).toDouble)
    weightContributions(contributions)(trustEntropyMap)
  }

  // TODO: Move to RewardsManager
  def weightContributions(
    contributions: Map[String, Double]
  )(trustEntropyMap: Map[String, Double]): Map[String, Double] = {
    val weightedEntropy = contributions.transform {
      case (address, partitionSize) =>
        partitionSize * (1 - trustEntropyMap(address)) // Normalize wrt total partition space
    }
    val totalEntropy = weightedEntropy.values.sum
    weightedEntropy.mapValues(_ / totalEntropy) // Scale by entropy magnitude
  }

  /*
 If nodes deviate more than x% from the accepted checkpoint block, drop to 0?
   */
  def performanceExperience(cps: Seq[MetaCheckpointBlock]): Map[Int, Double] = {
    val facilDiffsPerRound = cps.flatMap { cb =>
      val acceptedCbHashes = cb.transactions.map(_.hash).toSet //todo combine in all data hashes, currently tx check
      cb.proposals.mapValues { proposedCb: Set[String] =>
        val diff = acceptedCbHashes.diff(proposedCb)
        diff.size
      }
    }
    val facilDiffs
      : Map[Int, Seq[(Int, Int)]] = facilDiffsPerRound.groupBy { case (facilitator, diff) => facilitator } //.mapValues(v => v / cpb.size.toDouble)
    facilDiffs.mapValues { fd =>
      val acceptedCbTxHashes = cps.flatMap(_.transactions.map(_.hash).toSet)
      fd.map(_._2).sum / acceptedCbTxHashes.size.toDouble

    }
  }

  case class TestCheckpointBlock(proposals: Map[Int, Set[String]], acceptedCb: Set[String])

  case class MetaCheckpointBlock(
    proposals: Map[Int, Set[String]], //todo use Id instead of Int, see below
    trustEdges: Option[Map[Int, Seq[TrustEdge]]], //todo use actual Ids in SAW
    transactions: Seq[Transaction],
    checkpoint: CheckpointEdge,
    messages: Seq[ChannelMessage] = Seq(),
    notifications: Seq[PeerNotification] = Seq()
  ) //extends CheckpointBlock(transactions, checkpoint)
}
