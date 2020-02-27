package org.constellation.checkpoint

import cats.effect.Sync
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.primitives.Schema.{CheckpointCacheMetadata, Height, SignedObservationEdge}
import org.constellation.primitives.{CheckpointBlock, Genesis}
import org.constellation.storage.SOEService

class CheckpointParentService[F[_]: Sync](
  val soeService: SOEService[F],
  checkpointService: CheckpointService[F],
  dao: DAO
) {
  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def parentBaseHashesDirect(cb: CheckpointBlock): List[String] =
    cb.parentSOEHashes.toList.traverse { soeHash =>
      if (soeHash == Genesis.Coinbase) {
        none[String]
      } else {
        cb.checkpoint.edge.observationEdge.parents.find(_.hashReference == soeHash).flatMap(_.baseHash)
      }
    }.getOrElse(List.empty)

  def parentSOEBaseHashes(cb: CheckpointBlock): F[List[String]] =
    cb.parentSOEHashes.toList.traverse { soeHash =>
      if (soeHash == Genesis.Coinbase) {
        Sync[F].pure[Option[String]](None)
      } else {
        soeService.lookup(soeHash).flatMap { parent =>
          if (parent.isEmpty) {
            logger
              .debug(s"SOEHash $soeHash missing from soeService for cb: ${cb.baseHash}")
              .flatTap(_ => dao.metrics.incrementMetricAsync("parentSOEServiceQueryFailed"))
              .map(_ => cb.checkpoint.edge.observationEdge.parents.find(_.hashReference == soeHash).flatMap { _.baseHash })
              .flatTap(
                parentDirect =>
                  if (parentDirect.isEmpty) dao.metrics.incrementMetricAsync("parentDirectTipReferenceMissing")
                  else Sync[F].unit
              )
          } else Sync[F].delay(parent.map(_.baseHash))
        }
      }
    }.map(_.flatten)

  def calculateHeight(cb: CheckpointBlock): F[Option[Height]] =
    parentSOEBaseHashes(cb)
      .flatTap(
        l =>
          if (l.isEmpty) dao.metrics.incrementMetricAsync("heightCalculationSoeBaseMissing")
          else Sync[F].unit
      )
      .flatMap { soeBaseHashes =>
        soeBaseHashes.traverse(checkpointService.lookup)
      }
      .map { parents =>
        val maybeHeight = parents.flatMap(_.flatMap(_.height))
        if (maybeHeight.isEmpty) None else Some(Height(maybeHeight.map(_.min).min + 1, maybeHeight.map(_.max).max + 1))
      }
      .flatTap(h => if (h.isEmpty) dao.metrics.incrementMetricAsync("heightCalculationParentMissing") else Sync[F].unit)

  def getParents(c: CheckpointBlock): F[List[CheckpointBlock]] =
    parentSOEBaseHashes(c)
      .flatTap(
        l =>
          if (l.size != 2) dao.metrics.incrementMetricAsync("validationParentSOEBaseHashesMissing")
          else Sync[F].unit
      )
      .flatMap(_.traverse(checkpointService.fullData))
      .flatTap(
        cbs =>
          if (cbs.exists(_.isEmpty)) dao.metrics.incrementMetricAsync("validationParentCBLookupMissing")
          else Sync[F].unit
      )
      .map(_.flatMap(_.map(_.checkpointBlock)))

  def incrementChildrenCount(checkpointBlock: CheckpointBlock): F[List[Option[CheckpointCacheMetadata]]] =
    parentSOEBaseHashes(checkpointBlock).flatMap(_.traverse { hash =>
      checkpointService.update(hash, (cd: CheckpointCacheMetadata) => cd.copy(children = cd.children + 1))
    })

  def lookupCheckpoint(key: String): F[Option[CheckpointCacheMetadata]] = checkpointService.lookup(key)

}
