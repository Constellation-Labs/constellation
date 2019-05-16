package org.constellation.primitives
import cats.effect.IO
import com.typesafe.scalalogging.Logger
import org.constellation.DAO
import org.constellation.consensus.TipData
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.util.Metrics

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Random, Success, Try}

trait ConcurrentTipService {

  def getMinTipHeight()(implicit dao: DAO): Long
  def toMap: Map[String, TipData]
  def size: Int
  def set(tips: Map[String, TipData])
  def update(checkpointBlock: CheckpointBlock)(implicit dao: DAO): IO[Either[TipConflictException, Option[TipData]]]
  def pull(
    readyFacilitators: Map[Id, PeerData]
  )(implicit metrics: Metrics): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])]
  def markAsConflict(key: String)(implicit metrics: Metrics): Unit

}

case class TipConflictException(cb: CheckpointBlock)
      extends Exception(
        s"CB with baseHash: ${cb.baseHash} is conflicting with other tip or its ancestor."
      )
class TrieBasedTipService(sizeLimit: Int,
                          maxWidth: Int,
                          numFacilitatorPeers: Int,
                          minPeerTimeAddedSeconds: Int)(implicit dao: DAO)
    extends ConcurrentTipService {

  private val conflictingTips: TrieMap[String, CheckpointBlock] = TrieMap.empty
  private val tips: TrieMap[String, TipData] = TrieMap.empty
  private val logger = Logger("TrieBasedTipService")

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

  def remove(key: String)(implicit metrics: Metrics): Unit = tips.synchronized {
    tips -= key
    metrics.incrementMetric("checkpointTipsRemoved")
  }

  def markAsConflict(key: String)(implicit metrics: Metrics): Unit = {
    logger.warn(s"Marking tip as conflicted tipHash: $key")

    tips.get(key).foreach { tip =>
      tips -= key
      metrics.incrementMetric("conflictTipRemoved")
      conflictingTips.put(key, tip.checkpointBlock)
    }
  }

  def update(checkpointBlock: CheckpointBlock)(implicit dao: DAO): IO[Either[TipConflictException, Option[TipData]]] = {
    IO {
      tips.synchronized {
        val start = System.currentTimeMillis
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

        val elapsed1 = System.currentTimeMillis - start
        logger.info(s"--- --- TrieBased Elapsed1: ${elapsed1}ms")

        if (!CheckpointBlockValidatorNel.isConflictingWithOthers(
          checkpointBlock,
          tips.map(_._2.checkpointBlock).toSeq
        )) {
          val elapsed2 = (System.currentTimeMillis - start)
          logger.info(s"--- --- TrieBased Elapsed2: ${elapsed2}ms")
          Right(put(checkpointBlock.baseHash, TipData(checkpointBlock, 0))(dao.metrics))
        } else {
          logger.warn(s"Unable to add conflicted checkpoint block: ${checkpointBlock.baseHash}" )
          conflictingTips.put(checkpointBlock.baseHash, checkpointBlock)
          val elapsed3 = (System.currentTimeMillis - start)
          logger.info(s"--- --- TrieBased Elapsed3: ${elapsed3}ms")
          Left(TipConflictException(checkpointBlock))
        }

        Right(put(checkpointBlock.baseHash, TipData(checkpointBlock, 0))(dao.metrics))

      }
    }
  }


  private def put(k: String, v: TipData)(implicit metrics: Metrics): Option[TipData] = {

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

    if (maybeDatas.exists { _.isEmpty }) {
      dao.metrics.incrementMetric("minTipHeightCBDataEmptyForKeys")
    }

    maybeDatas.flatMap {
      _.flatMap {
        _.height.map {
          _.min
        }
      }
    }.min

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

  private def calculateTipsSOE(): Seq[SignedObservationEdge] = {
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
