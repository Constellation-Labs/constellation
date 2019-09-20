package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import cats.effect.concurrent.Semaphore
import cats.effect.{Clock, Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.consensus.TipData
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.{Height, Id, SignedObservationEdge}
import org.constellation.primitives.concurrency.{SingleLock, SingleRef}
import org.constellation.util.Logging._
import org.constellation.util.Metrics
import org.constellation.{ConstellationExecutionContext, DAO}

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

class ConcurrentTipService[F[_]: Concurrent: Logger: Clock](
  sizeLimit: Int,
  maxWidth: Int,
  maxTipUsage: Int,
  numFacilitatorPeers: Int,
  minPeerTimeAddedSeconds: Int,
  dao: DAO,
  facilitatorFilter: FacilitatorFilter[F]
) {

  def clearStaleTips(min: Long): F[Unit] =
    tipsRef.get.map(tips => tips.filter(_._2.height.min < min)).flatMap { toRemove =>
      Logger[F]
        .debug(s"Removing tips that are below cluster height: $min to remove ${toRemove.map(t => (t._1, t._2.height))}")
        .flatMap(_ => tipsRef.update(curr => curr -- toRemove.keySet))
    }

  private val conflictingTips: SingleRef[F, Map[String, CheckpointBlock]] = SingleRef(Map.empty)
  private val tipsRef: SingleRef[F, Map[String, TipData]] = SingleRef(Map.empty)
  private val semaphore: Semaphore[F] = {
    implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
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

  def removeUnsafe(key: String)(implicit metrics: Metrics): F[Unit] =
    tipsRef.updateUnsafe(_ - key).flatTap(_ => metrics.incrementMetricAsync("checkpointTipsRemoved"))

  def markAsConflict(key: String)(implicit metrics: Metrics): F[Unit] =
    logThread(
      get(key).flatMap { m =>
        if (m.isDefined)
          remove(key)
            .flatMap(_ => conflictingTips.update(_ + (key -> m.get.checkpointBlock)))
            .flatTap(_ => Logger[F].warn(s"Marking tip as conflicted tipHash: $key"))
        else Sync[F].unit
      },
      "concurrentTipService_markAsConflict"
    )

  def update(checkpointBlock: CheckpointBlock, height: Height, isGenesis: Boolean = false)(implicit dao: DAO): F[Unit] =
    withLock("updateTips", updateUnsafe(checkpointBlock, height: Height, isGenesis))

  def updateUnsafe(checkpointBlock: CheckpointBlock, height: Height, isGenesis: Boolean = false)(
    implicit dao: DAO
  ): F[Unit] = {
    val tipUpdates = checkpointBlock.parentSOEBaseHashes.distinct.toList.traverse { h =>
      for {
        tipData <- getUnsafe(h)
        size <- sizeUnsafe
        reuseTips = size < maxWidth
        aboveMinimumTip = size > numFacilitatorPeers
        _ <- tipData match {
          case None => Sync[F].unit
          case Some(TipData(block, numUses, _)) if aboveMinimumTip && (numUses >= maxTipUsage || !reuseTips) =>
            removeUnsafe(block.baseHash)(dao.metrics)
          case Some(TipData(block, numUses, tipHeight)) =>
            putUnsafe(block.baseHash, TipData(block, numUses + 1, tipHeight))(dao.metrics)
              .flatMap(_ => dao.metrics.incrementMetricAsync("checkpointTipsIncremented"))
        }
      } yield ()
    }

    logThread(
      tipUpdates
        .flatMap(_ => getMinTipHeight(None))
        .flatMap(
          min =>
            if (isGenesis || min < height.min)
              putUnsafe(checkpointBlock.baseHash, TipData(checkpointBlock, 0, height))(dao.metrics)
            else Logger[F].debug(s"Block height: ${height.min} below min tip: $min update skipped")
        )
        .recoverWith {
          case err: TipThresholdException =>
            dao.metrics
              .incrementMetricAsync("memoryExceeded_thresholdMetCheckpoints")
              .flatMap(_ => sizeUnsafe)
              .flatMap(s => dao.metrics.updateMetricAsync("activeTips", s))
              .flatMap(_ => Sync[F].raiseError[Unit](err))
        },
      "concurrentTipService_updateUnsafe"
    )
  }

  def putConflicting(k: String, v: CheckpointBlock)(implicit dao: DAO): F[Unit] = {
    val unsafePut = for {
      size <- conflictingTips.getUnsafe.map(_.size)
      _ <- dao.metrics
        .updateMetricAsync("conflictingTips", size)
      _ <- conflictingTips.updateUnsafe(_ + (k -> v))
    } yield ()

    logThread(withLock("conflictingPut", unsafePut), "concurrentTipService_putConflicting")
  }

  private def putUnsafe(k: String, v: TipData)(implicit metrics: Metrics): F[Unit] =
    sizeUnsafe.flatMap(
      size =>
        if (size < sizeLimit) tipsRef.updateUnsafe(curr => curr + (k -> v))
        else Sync[F].raiseError[Unit](TipThresholdException(v.checkpointBlock, sizeLimit))
    )

  def getMinTipHeight(minActiveTipHeight: Option[Long])(implicit dao: DAO): F[Long] =
    logThread(
      for {
        _ <- Logger[F].debug(s"Active tip height: $minActiveTipHeight")
        keys <- tipsRef.get.map(_.keys.toList)
        maybeData <- LiftIO[F].liftIO(keys.traverse(dao.checkpointService.lookup(_)))
        diff = keys.diff(maybeData.flatMap(_.map(_.checkpointBlock.baseHash)))
        _ <- if (diff.nonEmpty) Logger[F].debug(s"wkoszycki not_mapped ${diff}") else Sync[F].unit
        heights = maybeData.flatMap {
          _.flatMap {
            _.height.map {
              _.min
            }
          }
        } ++ minActiveTipHeight.toList
        minHeight = if (heights.isEmpty) 0 else heights.min
      } yield minHeight,
      "concurrentTipService_getMinTipHeight"
    )

  def pull(readyFacilitators: Map[Id, PeerData])(implicit metrics: Metrics): F[Option[PulledTips]] =
    logThread(
      tipsRef.get.flatMap { tips =>
        metrics.updateMetric("activeTips", tips.size)
        (tips.size, readyFacilitators) match {
          case (size, facilitators) if size >= numFacilitatorPeers && facilitators.nonEmpty =>
            val tipSOE = calculateTipsSOE(tips)
            facilitatorFilter
              .filterPeers(facilitators, tips, numFacilitatorPeers)
              .map(f => {
                Some(PulledTips(tipSOE, calculateFinalFacilitators(f, tipSOE.soe.map(_.hash).reduce(_ + _))))
              })
          case (size, _) if size >= numFacilitatorPeers =>
            Sync[F].pure(Some(PulledTips(calculateTipsSOE(tips), Map.empty[Id, PeerData])))
          case (_, _) => Sync[F].pure(None)
        }
      },
      "concurrentTipService_pull"
    )

  private def calculateTipsSOE(tips: Map[String, TipData]): TipSoe = {
    val r = Random
      .shuffle(if (tips.size > 50) tips.slice(0, 50).toSeq else tips.toSeq)
      .take(numFacilitatorPeers)
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
