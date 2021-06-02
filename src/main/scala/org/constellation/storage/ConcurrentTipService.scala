package org.constellation.storage

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Clock, Concurrent, LiftIO, Sync}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.checkpoint.CheckpointParentService
import org.constellation.concurrency.SingleLock
import org.constellation.consensus.FacilitatorFilter
import org.constellation.p2p.PeerData
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, FinishedCheckpoint, TipData}
import org.constellation.schema.edge.SignedObservationEdge
import org.constellation.schema.{Height, Id, checkpoint}
import org.constellation.util.Logging._
import org.constellation.util.Metrics
import org.constellation.{ConfigUtil, DAO}

import scala.util.{Random, Try}

case class TipSoe(soe: Seq[SignedObservationEdge], minHeight: Option[Long])

object TipSoe {
  implicit val tipSoeEncoder: Encoder[TipSoe] = deriveEncoder
  implicit val tipSoeDecoder: Decoder[TipSoe] = deriveDecoder
}

case class PulledTips(tipSoe: TipSoe, peers: Map[Id, PeerData])
case class TipConflictException(cb: CheckpointBlock, conflictingTxs: List[String])
    extends Exception(
      s"CB with baseHash: ${cb.baseHash} is conflicting with other tip or its ancestor. With following txs: $conflictingTxs"
    )
case class TipThresholdException(cb: CheckpointBlock, limit: Int)
    extends Exception(
      s"Unable to add CB with baseHash: ${cb.baseHash} as tip. Current tips limit met: $limit"
    )

class ConcurrentTipService[F[_]: Concurrent: Clock](
  sizeLimit: Int,
  maxWidth: Int,
  maxTipUsage: Int,
  numFacilitatorPeers: Int,
  minPeerTimeAddedSeconds: Int,
  checkpointParentService: CheckpointParentService[F],
  dao: DAO,
  facilitatorFilter: FacilitatorFilter[F],
  metrics: Metrics
) {

  implicit val logger = Slf4jLogger.getLogger[F]

  private val snapshotHeightInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  def clearStaleTips(min: Long): F[Unit] =
    (min > snapshotHeightInterval)
      .pure[F]
      .ifM(
        tipsRef.get.map(tips => tips.filter(_._2.height.min <= min)).flatMap { toRemove =>
          Logger[F]
            .debug(
              s"Removing tips that are below cluster height: $min to remove ${toRemove.map(t => (t._1, t._2.height))}"
            )
            .flatMap(_ => tipsRef.modify(curr => (curr -- toRemove.keySet, ())))
        },
        Logger[F].debug(
          s"Min=${min} is lower or equal snapshotHeightInterval=${snapshotHeightInterval}. Skipping tips removal"
        )
      )

  private val conflictingTips: Ref[F, Map[String, CheckpointBlock]] = Ref.unsafe(Map.empty)
  private val tipsRef: Ref[F, Map[String, TipData]] = Ref.unsafe(Map.empty)
  private val semaphore: Semaphore[F] = createSemaphore()

  private val usages: Ref[F, Map[String, Set[String]]] = Ref.unsafe(Map.empty)

  private def withLock[R](name: String, thunk: F[R]) = new SingleLock[F, R](name, semaphore).use(thunk)

  def set(newTips: Map[String, TipData]): F[Unit] =
    tipsRef.modify(_ => (newTips, ()))

  def toMap: F[Map[String, TipData]] =
    tipsRef.get

  def size: F[Int] =
    tipsRef.get.map(_.size)

  def get(key: String): F[Option[TipData]] =
    tipsRef.get.map(_.get(key))

  def remove(key: String)(implicit metrics: Metrics): F[Unit] =
    tipsRef.modify(t => (t - key, ())).flatTap(_ => metrics.incrementMetricAsync("checkpointTipsRemoved"))

  def markAsConflict(key: String)(implicit metrics: Metrics): F[Unit] =
    logThread(
      get(key).flatMap { m =>
        if (m.isDefined)
          remove(key)
            .flatMap(_ => conflictingTips.modify(c => (c + (key -> m.get.checkpointBlock), ())))
            .flatTap(_ => logger.warn(s"Marking tip as conflicted tipHash: $key"))
        else Sync[F].unit
      },
      "concurrentTipService_markAsConflict"
    )

  def update(checkpointBlock: CheckpointBlock, height: Height, isGenesis: Boolean = false): F[Unit] =
    countUsages(checkpointBlock.soeHash) >>= { usages =>
      withLock("updateTips", updateUnsafe(checkpointBlock, height, usages, isGenesis))
    }

  def updateUnsafe(checkpointBlock: CheckpointBlock, height: Height, usages: Int, isGenesis: Boolean = false): F[Unit] = {
    val tipUpdates = checkpointParentService
      .parentSOEBaseHashes(checkpointBlock)
      .flatMap(
        l =>
          l.distinct.traverse { h =>
            for {
              tipData <- get(h)
              size <- size
              reuseTips = size < maxWidth
              areEnoughTipsForConsensus = size >= numFacilitatorPeers
              _ <- tipData match {
                case None => Sync[F].unit
                case Some(TipData(block, numUses, _)) if areEnoughTipsForConsensus && (numUses >= maxTipUsage || !reuseTips) =>
                  remove(block.baseHash)(dao.metrics)
                case Some(TipData(block, numUses, tipHeight)) =>
                  putUnsafe(block.baseHash, checkpoint.TipData(block, numUses + 1, tipHeight))(dao.metrics)
                    .flatMap(_ => dao.metrics.incrementMetricAsync("checkpointTipsIncremented"))
              }
            } yield ()
          }
      )

    logThread(
      tipUpdates
        .flatMap(_ => getMinTipHeight(None))
        .flatMap(
          min =>
            if (isGenesis || (min < height.min && usages < maxTipUsage))
              putUnsafe(checkpointBlock.baseHash, TipData(checkpointBlock, usages, height), true)(dao.metrics)
            else logger.debug(s"Block height: ${height.min} with usages=${usages} above the limit or below min tip: $min or both | update skipped")
        )
        .recoverWith {
          case err: TipThresholdException =>
            dao.metrics
              .incrementMetricAsync("memoryExceeded_thresholdMetCheckpoints")
              .flatMap(_ => size)
              .flatMap(s => dao.metrics.updateMetricAsync("activeTips", s))
              .flatMap(_ => Sync[F].raiseError[Unit](err))
        },
      "concurrentTipService_updateUnsafe"
    )
  }

  def putConflicting(k: String, v: CheckpointBlock): F[Unit] = {
    val unsafePut = for {
      size <- conflictingTips.get.map(_.size)
      _ <- dao.metrics
        .updateMetricAsync("conflictingTips", size)
      _ <- conflictingTips.modify(c => (c + (k -> v), ()))
    } yield ()

    logThread(withLock("conflictingPut", unsafePut), "concurrentTipService_putConflicting")
  }

  case class MinTipChecker(minTip: Long, inserted: Long)

  private def putUnsafe(k: String, v: TipData, isInsert: Boolean = false)(implicit metrics: Metrics): F[Unit] =
    size.flatMap(
      size =>
        if (size < sizeLimit)
          tipsRef.modify { curr =>
            val minTip: Long = Try(curr.values.map(_.height.min).min).toOption.getOrElse(0L)
            (curr + (k -> v), MinTipChecker(minTip, v.height.min))
          }
        else
          Sync[F].raiseError[MinTipChecker](TipThresholdException(v.checkpointBlock, sizeLimit))
    ).flatMap {
      case MinTipChecker(minTip, inserted) if minTip >= inserted && isInsert =>
        logger.error(s"putUnsafeMinTip: Wrong update[isInsert=$isInsert]! $minTip >= $inserted") >>
          metrics.incrementMetricAsync("wrongTipUpdate")
      case MinTipChecker(minTip, inserted) =>
        logger.debug(s"putUnsafeMinTip: Correct update[isInsert=$isInsert]! $minTip < $inserted")
    }

  def getMinTipHeight(minActiveTipHeight: Option[Long]): F[Long] =
    logThread(
      for {
        _ <- logger.debug(s"Active tip height: $minActiveTipHeight")
        keys <- tipsRef.get.map(_.keys.toList)
        maybeData <- keys.traverse(checkpointParentService.lookupCheckpoint)
        //waitingHeights <- LiftIO[F].liftIO(dao.checkpointAcceptanceService.awaiting.get).map(_.flatMap(_.height.map(_.min)).toList)

        diff = keys.diff(maybeData.flatMap(_.map(_.checkpointBlock.baseHash)))
        _ <- if (diff.nonEmpty) logger.debug(s"wkoszycki not_mapped ${diff}") else Sync[F].unit

        ourHeights = maybeData.flatMap {
          _.flatMap {
            _.height.map {
              _.min
            }
          }
        }// ++ waitingHeights
        ourMax = ourHeights.minimumOption.getOrElse(0L)
        heights = ourHeights ++ minActiveTipHeight.toList
        minHeight = if (heights.isEmpty) 0 else heights.min
        _ <- if (ourMax > minHeight)
          logger.warn(s"Our max tip height greater then selected min tip! ourMax=$ourMax > minHeight=$minHeight consensusesMinTip=$minActiveTipHeight") >>
            metrics.incrementMetricAsync("localMaxTipGreater")
        else
          Sync[F].unit
      } yield minHeight,
      "concurrentTipService_getMinTipHeight"
    )

  def pull(readyFacilitators: Map[Id, PeerData])(implicit metrics: Metrics): F[Option[PulledTips]] =
    logThread(
      tipsRef.get.flatMap { tips =>
        metrics.updateMetric("activeTips", tips.size)
        (tips.size, readyFacilitators) match {
          case (size, facilitators) if size >= numFacilitatorPeers && facilitators.nonEmpty =>
            calculateTipsSOE(tips).flatMap { tipSOE =>
              facilitatorFilter.filterPeers(facilitators, numFacilitatorPeers, tipSOE).map {
                case f if f.size >= numFacilitatorPeers =>
                  Some(PulledTips(tipSOE, calculateFinalFacilitators(f, tipSOE.soe.map(_.hash).reduce(_ + _))))
                case _ => None
              }
            }
          case (size, _) if size >= numFacilitatorPeers =>
            calculateTipsSOE(tips).map(t => Some(PulledTips(t, Map.empty[Id, PeerData])))
          case (_, _) => none[PulledTips].pure[F]
        }
      },
      "concurrentTipService_pull"
    )

  def registerUsages(checkpoint: CheckpointCache): F[Unit] = {
    val parents = checkpoint.checkpointBlock.parentSOEHashes.distinct.toList
    val hash = checkpoint.checkpointBlock.soeHash

    parents.traverse { parent =>
      usages.modify { m =>
        val data = m.get(parent)
        val updated = data.map(_ ++ Set(hash)).getOrElse(Set(hash))
        (m.updated(parent, updated), ())
      }
    }.void
  }

  def registerUsages(checkpoint: FinishedCheckpoint): F[Unit] =
    registerUsages(checkpoint.checkpointCacheData)

  def countUsages(soeHash: String): F[Int] =
    usages.get.map(_.get(soeHash).map(_.size).getOrElse(0))

  def getUsages: F[Map[String, Set[String]]] =
    usages.get

  def setUsages(u: Map[String, Set[String]]): F[Unit] =
    usages.set(u)

  def batchRemoveUsages(cbs: Set[String]): F[Unit] =
    usages.modify { u =>
      (u -- cbs, ())
    }

  private def calculateTipsSOE(tips: Map[String, TipData]): F[TipSoe] =
    Random
      .shuffle(tips.toSeq.sortBy(_._2.height.min).take(10))
      .take(numFacilitatorPeers)
      .toList
      .traverse { t =>
        checkpointParentService
          .calculateHeight(t._2.checkpointBlock)
          .map(h => (h, t._2.checkpointBlock.checkpoint.edge.signedObservationEdge))
      }
      .map(_.sortBy(_._2.hash))
      .map(r => TipSoe(r.map(_._2), r.map(_._1.map(_.min)).min))

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
