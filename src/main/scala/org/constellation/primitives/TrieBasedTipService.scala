package org.constellation.primitives
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.constellation.DAO
import org.constellation.consensus.RoundManager.{ActiveTipMinHeight, GetActiveMinHeight}
import org.constellation.consensus.TipData
import org.constellation.primitives.Schema.{Height, Id, SignedObservationEdge}
import org.constellation.util.Metrics
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.concurrent.TrieMap
import scala.util.Random

trait ConcurrentTipService {

  def getMinTipHeight()(implicit dao: DAO): IO[Long]
  def toMap: Map[String, TipData]
  def size: Int
  def set(tips: Map[String, TipData])

  def update(checkpointBlock: CheckpointBlock)(
    implicit dao: DAO
  ): IO[Option[TipData]]
  def putConflicting(k: String, v: CheckpointBlock)(implicit dao: DAO): IO[Option[CheckpointBlock]]

  def pull(
    readyFacilitators: Map[Id, PeerData]
  )(implicit metrics: Metrics): Option[PulledTips]
  def markAsConflict(key: String)(implicit metrics: Metrics): Unit

}

case class TipSoe(soe: Seq[SignedObservationEdge], minHeight: Option[Long])
case class PulledTips(tipSoe: TipSoe, peers: Map[Id, PeerData])
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
                          minPeerTimeAddedSeconds: Int,
                          consensusActor: ActorRef)(
  implicit dao: DAO
) extends ConcurrentTipService {

  private val conflictingTips: TrieMap[String, CheckpointBlock] = TrieMap.empty
  private val tips: TrieMap[String, TipData] = TrieMap.empty
  private val logger = Logger("TrieBasedTipService")
  implicit var shortTimeout: Timeout = Timeout(3, TimeUnit.SECONDS)

  override def set(newTips: Map[String, TipData]): Unit =
    tips ++= newTips

  override def toMap: Map[String, TipData] =
    tips.toMap

  def size: Int =
    tips.size

  def get(key: String): IO[Option[TipData]] =
    IO(tips.get(key))

  def remove(key: String)(implicit metrics: Metrics): IO[Unit] =
    IO {
      tips.synchronized {
        tips -= key
      }
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
              .flatMap(_ => dao.metrics.incrementMetricAsync[IO]("checkpointTipsIncremented"))
        }
      } yield ()
    }

    tipUpdates
      .flatMap(_ => put(checkpointBlock.baseHash, TipData(checkpointBlock, 0))(dao.metrics))
      .recoverWith {
        case err: TipThresholdException =>
          dao.metrics
            .incrementMetricAsync[IO]("memoryExceeded_thresholdMetCheckpoints")
            .flatMap(_ => dao.metrics.updateMetricAsync[IO]("activeTips", tips.size))
            .flatMap(_ => IO.raiseError[Option[TipData]](err))
      }
  }

  def putConflicting(k: String, v: CheckpointBlock)(implicit dao: DAO): IO[Option[CheckpointBlock]] =
    dao.metrics
      .updateMetricAsync[IO]("conflictingTips", conflictingTips.size)
      .flatMap(_ => IO(conflictingTips.put(k, v)))

  private def put(k: String, v: TipData)(implicit metrics: Metrics): IO[Option[TipData]] =
    IO {
      tips.synchronized {
        if (tips.size < sizeLimit) {
          tips.put(k, v)
        } else {
          throw TipThresholdException(v.checkpointBlock, sizeLimit)
        }
      }
    }

  def getMinTipHeight()(implicit dao: DAO): IO[Long] = {

    val minimumActiveTipHeightTask: IO[ActiveTipMinHeight] = IO.async { cb =>
      import scala.util.{Failure, Success}

      (consensusActor ? GetActiveMinHeight)
        .mapTo[ActiveTipMinHeight]
        .onComplete {
          case Success(activeHeight) =>
            cb(Right(activeHeight))
          case Failure(error) => cb(Left(error))
        }(dao.edgeExecutionContext)
    }

    for {
      minActiveTipHeight <- minimumActiveTipHeightTask
      _ = logger.info(s"Active tip height: ${minActiveTipHeight.minHeight}")
      maybeDatas <- tips.keys.toList.traverse(dao.checkpointService.lookup(_))
      heights = maybeDatas.flatMap {
        _.flatMap {
          _.height.map {
            _.min
          }
        }
      } ++ minActiveTipHeight.minHeight.toList
      minHeight = if (heights.isEmpty) 0 else heights.min
    } yield minHeight
  }

  override def pull(
    readyFacilitators: Map[Id, PeerData]
  )(implicit metrics: Metrics): Option[PulledTips] = {

    metrics.updateMetric("activeTips", tips.size)

    (tips.size, readyFacilitators) match {
      case (x, facilitators) if x >= 2 && facilitators.nonEmpty =>
        val tipSOE = calculateTipsSOE()
        Some(PulledTips(tipSOE, calculateFinalFacilitators(facilitators, tipSOE.soe.map(_.hash).reduce(_ + _))))
      case (x, _) if x >= 2 =>
        Some(PulledTips(calculateTipsSOE(), Map.empty[Id, PeerData]))
      case (_, _) => None
    }
  }

  private def ensureTipsHaveParents(): Unit =
    tips.filterNot { z =>
      val parentHashes = z._2.checkpointBlock.parentSOEBaseHashes
      parentHashes.size == 2 && parentHashes.forall(dao.checkpointService.contains(_).unsafeRunSync())
    }.foreach {
      case (k, _) => tips.remove(k)
    }

  private def calculateTipsSOE(): TipSoe = {
    val r = Random
      .shuffle(if (size > 50) tips.slice(0, 50).toSeq else tips.toSeq)
      .take(2)
      .map { t =>
        (t._2.checkpointBlock.calculateHeight(), t._2.checkpointBlock.checkpoint.edge.signedObservationEdge)
      }
      .sortBy(_._2.hash)
    TipSoe(r.map(_._2), r.map(_._1.map(_.min)).min)
  }
  // ensureTipsHaveParents()

  private def calculateFinalFacilitators(facilitators: Map[Id, PeerData], mergedTipHash: String): Map[Id, PeerData] = {
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
