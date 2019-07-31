package org.constellation.primitives
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.consensus.TipData
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.primitives.concurrency.{SingleLock, SingleRef}
import org.constellation.util.Metrics
import org.constellation.{ConstellationContextShift, DAO}

import scala.util.Random

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

class ConcurrentTipService[F[_]: Concurrent: Logger](
  sizeLimit: Int,
  maxWidth: Int,
  maxTipUsage: Int,
  numFacilitatorPeers: Int,
  minPeerTimeAddedSeconds: Int,
  dao: DAO
) {

  private val conflictingTips: SingleRef[F, Map[String, CheckpointBlock]] = SingleRef(Map.empty)
  private val tipsRef: SingleRef[F, Map[String, TipData]] = SingleRef(Map.empty)
  private val semaphore: Semaphore[F] = {
    implicit val cs: ContextShift[IO] = ConstellationContextShift.edge
    Semaphore.in[IO, F](1).unsafeRunSync()
  }

  private def withLock[R](name: String, thunk: F[R]) = new SingleLock[F, R](name, semaphore).use(thunk)

  implicit var shortTimeout: Timeout = Timeout(3, TimeUnit.SECONDS)

  def set(newTips: Map[String, TipData]): F[Unit] =
    tipsRef.set(newTips)

  def toMap: F[Map[String, TipData]] =
    tipsRef.get

  def size: F[Int] =
    tipsRef.get.map(_.size)

  def sizeUnsafe: F[Int] =
    tipsRef.getUnsafe.map(_.size)

  def get(key: String): F[Option[TipData]] =
    tipsRef.get.map(_.get(key))

  def getUnsafe(key: String): F[Option[TipData]] =
    tipsRef.getUnsafe.map(_.get(key))

  def remove(key: String)(implicit metrics: Metrics): F[Unit] =
    tipsRef.update(_ - key).flatTap(_ => metrics.incrementMetricAsync("checkpointTipsRemoved"))

  def markAsConflict(key: String)(implicit metrics: Metrics): F[Unit] =
    get(key).flatMap { m =>
      if (m.isDefined)
        remove(key)
          .flatMap(_ => conflictingTips.update(_ + (key -> m.get.checkpointBlock)))
          .flatTap(_ => Logger[F].warn(s"Marking tip as conflicted tipHash: $key"))
      else Sync[F].unit
    }

  def update(checkpointBlock: CheckpointBlock)(implicit dao: DAO): F[Unit] =
    withLock("updateTips", updateUnsafe(checkpointBlock))

  def updateUnsafe(checkpointBlock: CheckpointBlock)(implicit dao: DAO): F[Unit] = {
    val tipUpdates = checkpointBlock.parentSOEBaseHashes.distinct.toList.traverse { h =>
      for {
        tipData <- getUnsafe(h)
        size <- sizeUnsafe
        reuseTips = size < maxWidth
        _ <- tipData match {
          case None => Sync[F].unit
          case Some(TipData(block, numUses)) if !reuseTips || numUses >= maxTipUsage =>
            remove(block.baseHash)(dao.metrics)
          case Some(TipData(block, numUses)) if reuseTips && numUses <= maxTipUsage =>
            putUnsafe(block.baseHash, TipData(block, numUses + 1))(dao.metrics)
              .flatMap(_ => dao.metrics.incrementMetricAsync("checkpointTipsIncremented"))
        }
      } yield ()
    }

    tipUpdates
      .flatMap(_ => putUnsafe(checkpointBlock.baseHash, TipData(checkpointBlock, 0))(dao.metrics))
      .recoverWith {
        case err: TipThresholdException =>
          dao.metrics
            .incrementMetricAsync("memoryExceeded_thresholdMetCheckpoints")
            .flatMap(_ => sizeUnsafe)
            .flatMap(s => dao.metrics.updateMetricAsync("activeTips", s))
            .flatMap(_ => Sync[F].raiseError[Unit](err))
      }
  }

  def putConflicting(k: String, v: CheckpointBlock)(implicit dao: DAO): F[Unit] = {
    val unsafePut = for {
      size <- conflictingTips.getUnsafe.map(_.size)
      _ <- dao.metrics
        .updateMetricAsync("conflictingTips", size)
      _ <- conflictingTips.updateUnsafe(_ + (k -> v))
    } yield ()

    withLock("conflictingPut", unsafePut)
  }

  private def putUnsafe(k: String, v: TipData)(implicit metrics: Metrics): F[Unit] =
    sizeUnsafe.flatMap(
      size =>
        if (size < sizeLimit) tipsRef.updateUnsafe(curr => curr + (k -> v))
        else Sync[F].raiseError[Unit](TipThresholdException(v.checkpointBlock, sizeLimit))
    )

  def getMinTipHeight(minActiveTipHeight: Option[Long])(implicit dao: DAO): F[Long] =
    for {
      _ <- Logger[F].debug(s"Active tip height: $minActiveTipHeight")
      keys <- tipsRef.get.map(_.keys.toList)
      maybeData <- LiftIO[F].liftIO(keys.traverse(dao.checkpointService.lookup(_)))
      heights = maybeData.flatMap {
        _.flatMap {
          _.height.map {
            _.min
          }
        }
      } ++ minActiveTipHeight.toList
      minHeight = if (heights.isEmpty) 0 else heights.min
    } yield minHeight

  def pull(readyFacilitators: Map[Id, PeerData])(implicit metrics: Metrics): F[Option[PulledTips]] =
    tipsRef.get.map { tips =>
      metrics.updateMetric("activeTips", tips.size)
      (tips.size, readyFacilitators) match {
        case (size, facilitators) if size >= 2 && facilitators.nonEmpty =>
          val tipSOE = calculateTipsSOE(tips)
          Some(PulledTips(tipSOE, calculateFinalFacilitators(facilitators, tipSOE.soe.map(_.hash).reduce(_ + _))))
        case (size, _) if size >= 2 =>
          Some(PulledTips(calculateTipsSOE(tips), Map.empty[Id, PeerData]))
        case (_, _) => None
      }
    }

  private def calculateTipsSOE(tips: Map[String, TipData]): TipSoe = {
    val r = Random
      .shuffle(if (tips.size > 50) tips.slice(0, 50).toSeq else tips.toSeq)
      .take(2)
      .map { t =>
        (t._2.checkpointBlock.calculateHeight()(dao), t._2.checkpointBlock.checkpoint.edge.signedObservationEdge)
      }
      .sortBy(_._2.hash)
    TipSoe(r.map(_._2), r.map(_._1.map(_.min)).min)
  }

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
