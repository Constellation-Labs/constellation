package org.constellation.checkpoint

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.schema.checkpoint.{CheckpointCache, CheckpointCacheMetadata}
import org.constellation.schema.transaction.Transaction
import org.constellation.storage.{ConcurrentStorageService, StorageService}
import org.constellation.storage.algebra.Lookup
import org.constellation.{ConstellationExecutionContext, DAO}

import scala.concurrent.duration._

class CheckpointService[F[_]: Concurrent](
  dao: DAO,
  merkleService: CheckpointMerkleService[F]
) {

  private val semaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()

  private[checkpoint] val memPool =
    new ConcurrentStorageService[F, CheckpointCacheMetadata](semaphore, "CheckpointMemPool".some)

  val gossipCache = new StorageService[F, CheckpointCache](Some("CheckpointGossipPool"), Some(10.minutes))

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def update(
    baseHash: String,
    update: CheckpointCacheMetadata => CheckpointCacheMetadata
  ): F[Option[CheckpointCacheMetadata]] =
    memPool.update(baseHash, update)

  def put(cbCache: CheckpointCache): F[CheckpointCacheMetadata] =
    merkleService
      .storeMerkleRoots(cbCache.checkpointBlock)
      .flatMap(
        ccm =>
          memPool
            .put(cbCache.checkpointBlock.baseHash, CheckpointCacheMetadata(ccm, cbCache.children, cbCache.height))
      )

  def batchRemove(cbs: List[String]): F[Unit] =
    logger
      .debug(s"[${dao.id.short}] applying snapshot for blocks: $cbs from others")
      .flatMap(_ => cbs.map(memPool.remove).sequence.void)

  def fetchBatchTransactions(merkleRoot: String): F[List[Transaction]] =
    merkleService.fetchBatchTransactions(merkleRoot)

  def fullData(key: String): F[Option[CheckpointCache]] =
    lookup(key).flatMap(_.map(merkleService.convert).sequence)

  def lookup(key: String): F[Option[CheckpointCacheMetadata]] =
    Lookup.extendedLookup[F, String, CheckpointCacheMetadata](List(memPool))(key)

  def contains(key: String): F[Boolean] = lookup(key).map(_.nonEmpty)

}
