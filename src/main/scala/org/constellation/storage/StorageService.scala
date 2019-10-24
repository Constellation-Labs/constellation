package org.constellation.storage

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage.algebra.StorageAlgebra
import org.constellation.util.Metrics

import scala.collection.immutable.Queue
import scala.concurrent.duration._

class StorageService[F[_]: Concurrent, V](
  metricName: Option[String] = None,
  expireAfterMinutes: Option[Int] = Some(240)
) extends StorageAlgebra[F, String, V] {

  private val lruCache: Cache[String, V] = {
    val cacheWithStats = metricName.fold(Scaffeine())(_ => Scaffeine().recordStats())

    val cache = expireAfterMinutes
      .map(mins => cacheWithStats.expireAfterAccess(mins.minutes))
      .getOrElse(cacheWithStats)

    cache.build[String, V]()
  }

  private val queueRef: SingleRef[F, Queue[V]] = SingleRef[F, Queue[V]](Queue[V]())
  private val maxQueueSize = 20

  if (metricName.isDefined) {
    Metrics.cacheMetrics.addCache(metricName.get, lruCache.underlying)
  }

  def update(key: String, updateFunc: V => V, empty: => V): F[V] =
    lookup(key)
      .map(_.map(updateFunc).getOrElse(empty))
      .flatMap(v => putToCache(key, v))

  def update(key: String, updateFunc: V => V): F[Option[V]] =
    lookup(key)
      .flatMap(
        _.map(updateFunc)
          .traverse(v => putToCache(key, v))
      )

  private[storage] def putToCache(key: String, v: V): F[V] =
    Sync[F].delay(lruCache.put(key, v)).map(_ => v)

  def put(key: String, v: V): F[V] =
    queueRef.update {
      case q if q.size >= maxQueueSize => q.dequeue._2
      case q                           => q
    } >>
      queueRef.update(_.enqueue(v)) >>
      putToCache(key, v)

  def lookup(key: String): F[Option[V]] =
    Sync[F].delay(lruCache.getIfPresent(key))

  def remove(keys: Set[String]): F[Unit] =
    Sync[F].delay(lruCache.invalidateAll(keys))

  def contains(key: String): F[Boolean] =
    lookup(key).map(_.isDefined)

  def size(): F[Long] = Sync[F].delay(lruCache.estimatedSize())

  def toMap(): F[Map[String, V]] =
    Sync[F].delay(lruCache.asMap().toMap)

  def getLast20(): F[List[V]] =
    queueRef.get.map(_.reverse.toList)
}
