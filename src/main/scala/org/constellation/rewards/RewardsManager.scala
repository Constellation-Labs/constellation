package org.constellation.rewards

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.Snapshot
import org.constellation.domain.observation.Observation
import org.constellation.p2p.PeerNotification
import org.constellation.primitives.Schema.CheckpointEdge
import org.constellation.primitives.{ChannelMessage, Schema, Transaction}
import org.constellation.storage.AddressService
import org.constellation.trust.TrustEdge

case class RewardSnapshot(
  hash: String,
  height: Long,
  observations: Seq[Observation]
)

class RewardsManager[F[_]: Concurrent](
  eigenTrust: EigenTrust[F],
  checkpointService: CheckpointService[F],
  addressService: AddressService[F]
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
        s"[Rewards] Updating rewards by snapshot ${rewardSnapshot.hash} at height ${rewardSnapshot.height}"
      )
      _ <- Concurrent[F]
        .pure(rewardSnapshot.observations.isEmpty)
        .ifM(
          logger.debug(s"[Rewards] ${rewardSnapshot.hash} has no observations"),
          logger.debug(s"[Rewards] ${rewardSnapshot.hash} contains following observations:").flatMap { _ =>
            rewardSnapshot.observations.toList
              .map(_.signedObservationData.data)
              .traverse(o => logger.debug(s"[Rewards] Observation for ${o.id}, ${o.event}"))
              .void
          }
        )
      _ <- eigenTrust.retrain(rewardSnapshot.observations)
      trustMap <- eigenTrust.getTrustForAddresses

      weightContributions = weightByTrust(trustMap) _ >>> weightByEpoch(rewardSnapshot.height) >>> weightByStardust
      contributions = calculateContributions(trustMap.keySet.toSeq)
      distribution = weightContributions(contributions)
      normalizedDistribution = normalize(distribution)
      _ <- logger.debug(s"[Rewards] Distribution:")
      _ <- normalizedDistribution.toList.traverse {
        case (address, reward) => logger.debug(s"[Rewards] Address: ${address}, Reward: ${reward}")
      }

      _ <- updateAddressBalances(normalizedDistribution)
      _ <- lastRewardedHeight.modify(_ => (rewardSnapshot.height, ()))

      _ <- logger.debug(
        s"[Rewards] Rewarding by ${rewardSnapshot.hash} succeeded."
      )
    } yield ()

  private def normalize(contributions: Map[String, Double]): Map[String, Long] =
    contributions
      .mapValues(Schema.NormalizationFactor * _)
      .mapValues(_.toLong)

  def weightByStardust: Map[String, Double] => Map[String, Double] =
    StardustCollective.weightByStardust

  private def weightByEpoch(snapshotHeight: Long)(contributions: Map[String, Double]): Map[String, Double] =
    contributions.mapValues(_ * RewardsManager.rewardDuringEpoch(snapshotHeight))

  private def weightByTrust(
    trustEntropyMap: Map[String, Double]
  )(contributions: Map[String, Double]): Map[String, Double] = {
    val weightedEntropy = contributions.transform {
      case (id, partitionSize) =>
        partitionSize * (1 - trustEntropyMap(id)) // Normalize wrt total partition space
    }
    val totalEntropy = weightedEntropy.values.sum
    weightedEntropy.mapValues(_ / totalEntropy) // Scale by entropy magnitude
  }

  /**
    * Calculates non-weighted contributions so each address takes equal part which is calculated
    * as 1/totalSpace
    * @param ids
    * @return
    */
  private def calculateContributions(ids: Seq[String]): Map[String, Double] = {
    val totalSpace = ids.size
    val contribution = 1.0 / totalSpace
    ids.map { id =>
      id -> contribution
    }.toMap
  }

  private def observationsFromSnapshot(snapshot: Snapshot): F[Seq[Observation]] =
    snapshot.checkpointBlocks.toList
      .traverse(checkpointService.fullData)
      .map(_.flatten.flatMap(_.checkpointBlock.observations))

  private def updateAddressBalances(rewards: Map[String, Long]): F[Unit] =
    addressService.transferRewards(rewards).void
}

object RewardsManager {
  val roundingError = 0.000000000001

  /*
  Rewards computed about one snapshots per 3 minutes, i.e. about 14,600 snapshots per month.
  Snapshots for a first epoch of 2.5 years.
   */
  val epochOne = 438000 // = 14,600 snapshots per month * 12 months per year * 2.5 years
  val epochTwo = 876000 // = 2 * epochOne (5 years)
  val epochThree = 1314000 // = 3 * epochOne (7.5 years)
  val epochFour = 1752000 // = 4 * epochOne (10 years)
  val rewardsPool = epochFour
  /*
  10,000 units per month.
   */
  val epochOneRewards = 444.44 // = 28,444,444.44 per month @ ~45 seconds per snapshot
  val epochTwoRewards = 0.34246575342 // = epochOneRewards / 2
  val epochThreeRewards = 0.17123287671 // = epochTwoRewards / 2
  val epochFourRewards = 0.08561643835 // = epochThreeRewards / 2

  /*
  Partitioning of address space, light nodes have smaller basis that full.
  Normalizes rewards based on node size.
   */
  val partitonChart = Map[String, Set[String]]()

  /*
  Should come from reputation service.
   */
  val transitiveReputationMatrix = Map[String, Map[String, Double]]()
  val neighborhoodReputationMatrix = Map[String, Double]()

  // TODO: Move to RewardsManager
  def rewardDuringEpoch(curShapshot: Long) = curShapshot match {
    case num if num >= 0 && num < epochOne           => epochOneRewards
    case num if num >= epochOne && num < epochTwo    => epochTwoRewards
    case num if num >= epochTwo && num < epochThree  => epochThreeRewards
    case num if num >= epochThree && num < epochFour => epochFourRewards
    case _                                           => 0d
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
    distro.mapValues(_ * rewardDuringEpoch(curShapshot))
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
