package org.constellation.primitives
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.Logger
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
  def update(checkpointBlock: CheckpointBlock)(
    implicit dao: DAO
  ): IO[Option[TipData]]
  def putConflicting(k: String, v: CheckpointBlock)(implicit dao: DAO): IO[Option[CheckpointBlock]]
  def pull(
    readyFacilitators: Map[Id, PeerData]
  )(implicit metrics: Metrics): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])]
  def markAsConflict(key: String)(implicit metrics: Metrics): Unit

}

case class TipConflictException(cb: CheckpointBlock, conflictingTxs: List[String])
  extends Exception(
    s"CB with baseHash: ${cb.baseHash} is conflicting with other tip or its ancestor. With following txs: $conflictingTxs"
  )
case class TipThresholdException(cb: CheckpointBlock, limit: Int)
  extends Exception(
    s"Unable to add CB with baseHash: ${cb.baseHash} as tip. Current tips limit met: $limit"
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

  def get(key: String): IO[Option[TipData]] = {
    IO(tips.get(key))
  }

  def remove(key: String)(implicit metrics: Metrics): IO[Unit] = {
    IO {
      tips.synchronized {
        tips -= key
      }
      metrics.incrementMetric("checkpointTipsRemoved")
    }
  }

  def markAsConflict(key: String)(implicit metrics: Metrics): Unit = {
    logger.warn(s"Marking tip as conflicted tipHash: $key")

    tips.get(key).foreach { tip =>
      tips -= key
      metrics.incrementMetric("conflictTipRemoved")
      conflictingTips.put(key, tip.checkpointBlock)
    }
  }

  def update(checkpointBlock: CheckpointBlock)(implicit dao: DAO): IO[Option[TipData]] = {
    val tipUpdates = checkpointBlock.parentSOEBaseHashes.distinct.toList.traverse { h =>
      for {
        tipData <- get(h)
        reuseTips = tips.size < maxWidth
        _ <- tipData match {
          case None => IO.unit
          case Some(TipData(block, numUses)) if !reuseTips || numUses >= 2 =>
            remove(block.baseHash)(dao.metrics)
          case Some(TipData(block, numUses)) if reuseTips && numUses <= 2 =>
            put(block.baseHash, TipData(block, numUses + 1))(dao.metrics)
              .flatMap(_ => dao.metrics.incrementMetricAsync("checkpointTipsIncremented"))
        }
      } yield ()
    }

    tipUpdates
      .flatMap(_ => put(checkpointBlock.baseHash, TipData(checkpointBlock, 0))(dao.metrics))
      .recoverWith {
        case err: TipThresholdException =>
          dao.metrics
            .incrementMetricAsync("memoryExceeded_thresholdMetCheckpoints")
            .flatMap(_ => dao.metrics.updateMetricAsync("activeTips", tips.size))
            .flatMap(_ => IO.raiseError(err))
      }
  }

  def putConflicting(k: String,
    v: CheckpointBlock)(implicit dao: DAO): IO[Option[CheckpointBlock]] = {
    dao.metrics
      .updateMetricAsync("conflictingTips", conflictingTips.size)
      .flatMap(_ => IO(conflictingTips.put(k, v)))
  }

  private def put(k: String, v: TipData)(implicit metrics: Metrics): IO[Option[TipData]] = {
    IO {
      tips.synchronized {
        if (tips.size < sizeLimit) {
          tips.put(k, v)
        } else {
          throw TipThresholdException(v.checkpointBlock, sizeLimit)
        }
      }
    }
  }

  def getMinTipHeight()(implicit dao: DAO): Long = {

    if (tips.keys.isEmpty) {
      dao.metrics.incrementMetric("minTipHeightKeysEmpty")
    }

    val maybeDatas = tips.keys.map(dao.checkpointService.lookup(_).unsafeRunSync())

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

    metrics.updateMetric("activeTips", tips.size)

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
        parentHashes.size == 2 && parentHashes.forall(dao.checkpointService.contains(_).unsafeRunSync())
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
