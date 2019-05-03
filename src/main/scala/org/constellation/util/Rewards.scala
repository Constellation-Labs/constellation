package org.constellation.util
import org.constellation.primitives.Schema.{CheckpointEdge, Id}
import org.constellation.primitives.{ChannelMessage, CheckpointBlock, PeerNotification, Transaction}
import org.constellation.trust.{SelfAvoidingWalk, TrustEdge, TrustNode}

object Rewards {
  val roundingError = 0.000000000001

  val epochOne = 438000
  val epochTwo = 876000
  val epochThree = 1314000
  val epochFour = 1752000

  val epochOneRewards = 0.68493150684//10k/mo, 328.767123287/day
  val epochTwoRewards = 0.34246575342
  val epochThreeRewards = 0.17123287671
  val epochFourRewards = 0.08561643835

  /*
  Partitioning of address space, light nodes have smaller basis that full. Normalizes rewards based on node size
   */
  val partitonChart = Map[String, Set[String]]()

  /*
  Should come from reputation service
   */
  val transitiveReputationMatrix = Map[String, Map[String, Double]]()
  val neighborhoodReputationMatrix = Map[String, Double]()

  /*
  snapshots - 10yrs @ ~20/hr
   */
  val rewardsPool = 1752000

  def validatorRewards(
                        curShapshot: Int,
                        transitiveReputationMatrix: Map[String, Map[String, Double]],
                        neighborhoodReputationMatrix: Map[String, Double],
                        partitonChart: Map[String, Set[String]]
                      )= {
    val trustEntropyMap = shannonEntropy(transitiveReputationMatrix, neighborhoodReputationMatrix)
    val distro = rewardDistribution(partitonChart, trustEntropyMap)
    distro.mapValues(_ * rewardForEpoch(curShapshot))
  }

  def rewardForEpoch(curShapshot: Int) = curShapshot match {
    case num if num >= 0 && num < epochOne  => epochOneRewards
    case num if num >= 438000 && num < epochTwo => epochTwoRewards
    case num if num >= 876000 && num < epochThree => epochThreeRewards
    case num if num >= 1314000 && num < epochFour => epochFourRewards
    case _ => 0d
  }

  def shannonEntropy(
                      transitiveReputationMatrix: Map[String, Map[String, Double]],
                      neighborhoodReputationMatrix: Map[String, Double]
                    ) = {
    val weightedTransitiveReputation = transitiveReputationMatrix.map { case (key, view) =>
      val neighborView = view.map { case (neighbor, score) => neighborhoodReputationMatrix(key) * score }.sum
      (key, neighborView)
    }
    weightedTransitiveReputation.mapValues{ trust =>
      if (trust == 0.0 ) 0.0
      else - trust * math.log(trust)/math.log(2) }
  }

  def rewardDistribution(partitonChart: Map[String, Set[String]], trustEntropyMap: Map[String, Double]) = {
    val totalSpace = partitonChart.values.map(_.size).max
    val contributions = partitonChart.mapValues( partiton => partiton.size / totalSpace )
    val weightedEntropy = contributions.map { case (address, partitonSize) =>
      val reward = partitonSize * ( 1 - trustEntropyMap(address)) //normalize wrt total partition space
      (address, reward)
    }
    val totalEntropy = weightedEntropy.values.sum
    weightedEntropy.mapValues(_ / totalEntropy )//scale by entropy magnitude
  }

  /*
  If nodes deviate more than x% from the accepted checkpoint block, drop to 0?
   */
  def performanceExperience(cps: Seq[MetaCheckpointBlock]): Map[Int, Double] = {
    val facilDiffsPerRound = cps.flatMap { cb =>
    val acceptedCbHashes = cb.transactions.map(_.hash).toSet //todo combine in all data hashes, currently tx check
      cb.proposals.mapValues { proposedCb: Set[String] =>
      val diff = acceptedCbHashes diff proposedCb
        diff.size
      }
    }
    val facilDiffs: Map[Int, Seq[(Int, Int)]] = facilDiffsPerRound.groupBy { case (facilitator, diff) => facilitator}//.mapValues(v => v / cpb.size.toDouble)
    facilDiffs.mapValues {
      fd =>
        val acceptedCbTxHashes = cps.flatMap(_.transactions.map(_.hash).toSet)
        fd.map(_._2).sum / acceptedCbTxHashes.size.toDouble

    }
  }

  def updatedRewardsDistro(cps: Seq[MetaCheckpointBlock], curTrustDistro: Seq[TrustNode]) = {
    val experienceUpdates = performanceExperience(cps)
    val edgeUpdates = cps.flatMap(mcpb => mcpb.trustEdges).reduce(_ ++ _)
    val updatedTrustNodes = SelfAvoidingWalk.updateTrustDistro(curTrustDistro, edgeUpdates).toSeq
      //SelfAvoidingWalk.updateTrustDistro(curTrustDistro, cps.flatMap(_.trustEdges).flatten)
    val newWalk = SelfAvoidingWalk.getWalk(0, updatedTrustNodes)
    (newWalk, experienceUpdates)
  }

  case class TestCheckpointBlock(proposals: Map[Int, Set[String]], acceptedCb: Set[String])

  case class MetaCheckpointBlock(
                                  proposals: Map[Int, Set[String]],//todo use Id instead of Int, see below
                                  trustEdges: Option[Map[Int, Seq[TrustEdge]]],//todo use actual Ids in SAW
                                  transactions: Seq[Transaction],
                                   checkpoint: CheckpointEdge,
                                   messages: Seq[ChannelMessage] = Seq(),
                                   notifications: Seq[PeerNotification] = Seq()
                                ) //extends CheckpointBlock(transactions, checkpoint)

}
