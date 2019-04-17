package org.constellation.primitives
import org.constellation.DAO
import org.constellation.consensus.TipData
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.util.Metrics

import scala.collection.concurrent.TrieMap
import scala.util.Random

trait ConcurrentTipService {

  def getMinTipHeight()(implicit dao: DAO): Long
  def toMap: Map[String, TipData]
  def size: Int
  def set(tips: Map[String, TipData])
  def update(checkpointBlock: CheckpointBlock)(implicit dao: DAO)
  // considerd as private only
  def put(k: String, v: TipData)(implicit metrics: Metrics): Option[TipData]
  def pull(
    readyFacilitators: Map[Id, PeerData]
  )(implicit metrics: Metrics): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])]
  def get(k: String): Option[TipData]
  def remove(k: String)(implicit metrics: Metrics): Unit

}

class TrieBasedTipService(sizeLimit: Int,
                          maxWidth: Int,
                          numFacilitatorPeers: Int,
                          minPeerTimeAddedSeconds: Int) (implicit dao: DAO)
    extends ConcurrentTipService {

  private val tips: TrieMap[String, TipData] = TrieMap.empty

  override def set(newTips: Map[String, TipData]): Unit = {
    tips ++= newTips
  }

  override def toMap: Map[String, TipData] = {
    tips.toMap
  }

  def size: Int = {
    tips.size
  }

  def get(key: String): Option[TipData] = {
    tips.get(key)
  }

  def remove(key: String)(implicit metrics: Metrics): Unit = {
    tips -= key
    metrics.incrementMetric("checkpointTipsRemoved")
  }

  def update(checkpointBlock: CheckpointBlock)(implicit dao: DAO): Unit = {
    // TODO: should size of the map be calculated each time or just once?
    // previously it was static due to all method were sync and map updates
    // were performed on the end
//    def reuseTips: Boolean = tips.size < maxWidth
    val reuseTips: Boolean = tips.size < maxWidth

    checkpointBlock.parentSOEBaseHashes.distinct.foreach { h =>
      tips.get(h).foreach {
        case TipData(block, numUses) if !reuseTips || numUses >= 2 =>
          remove(block.baseHash)(dao.metrics)
        case TipData(block, numUses) if reuseTips && numUses <= 2 =>
          dao.metrics.incrementMetric("checkpointTipsIncremented")
          put(block.baseHash, TipData(block, numUses + 1))(dao.metrics)
      }
    }

    put(checkpointBlock.baseHash, TipData(checkpointBlock, 0))(dao.metrics)
  }

  def put(k: String, v: TipData)(implicit metrics: Metrics): Option[TipData] = {
    if (tips.size < sizeLimit) {
      tips.put(k, v)
    } else {
      // TODO: should newest override oldest cache-like? previously was just discarded
//      thresholdMetCheckpoints = thresholdMetCheckpoints.slice(0, 100)
      metrics.incrementMetric("memoryExceeded_thresholdMetCheckpoints")
      metrics.updateMetric("activeTips", tips.size.toString)
      None
    }
  }

  def getMinTipHeight()(implicit dao: DAO): Long = {

    if (tips.keys.isEmpty) {
      dao.metrics.incrementMetric("minTipHeightKeysEmpty")
    }

    val maybeDatas = tips.keys
      .map {
        dao.checkpointService.get
      }

    if (maybeDatas.exists{_.isEmpty}) {
      dao.metrics.incrementMetric("minTipHeightCBDataEmptyForKeys")
    }

    maybeDatas
      .flatMap {
        _.flatMap {
          _.height.map {
            _.min
          }
        }
      }
      .min

  }

  override def pull(
    readyFacilitators: Map[Id, PeerData]
  )(implicit metrics: Metrics): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])] = {

    metrics.updateMetric("activeTips", tips.size.toString)

    (tips.size, readyFacilitators) match {
      case (x, facilitators) if x >= 2 && facilitators.nonEmpty =>
        val tipSOE = calculateTipsSOE()
        Some(tipSOE -> calculateFinalFacilitators(facilitators, tipSOE.foldLeft("")(_ + _.hash)))
      case (x, _) if x >= 2 =>
        Some(calculateTipsSOE() -> Map.empty[Id, PeerData])
      case (_, _) => None
    }
  }

  private def ensureTipsHaveParents(): Unit = {
    tips.filterNot{
      z =>
        val parentHashes = z._2.checkpointBlock.parentSOEBaseHashes
        parentHashes.size == 2 && parentHashes.forall(dao.checkpointService.contains)
    }.foreach{
      case (k, _) => tips.remove(k)
    }
  }

  private def calculateTipsSOE(): Seq[SignedObservationEdge] = {

    // ensureTipsHaveParents()

    Random
      .shuffle(if (size > 50) tips.slice(0, 50).toSeq else tips.toSeq)
      .take(2)
      .map {
        _._2.checkpointBlock.checkpoint.edge.signedObservationEdge
      }
      .sortBy(_.hash)
  }
  private def calculateFinalFacilitators(facilitators: Map[Id, PeerData],
                                         mergedTipHash: String): Map[Id, PeerData] = {
    // TODO: Use XOR distance instead as it handles peer data mismatch cases better
    val facilitatorIndex = (BigInt(mergedTipHash, 16) % facilitators.size).toInt
    val sortedFacils = facilitators.toSeq.sortBy(_._1.hex)
    val selectedFacils = Seq
      .tabulate(numFacilitatorPeers) { i =>
        (i + facilitatorIndex) % facilitators.size
      }
      .map {
        sortedFacils(_)
      }
    selectedFacils.toMap
  }

}
