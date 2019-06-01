package org.constellation.storage

import cats.effect.concurrent.Ref
import cats.effect.Sync
import cats.implicits._
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.constellation.storage.algebra.StorageAlgebra
import org.constellation.util.Metrics

import scala.collection.immutable.Queue
import scala.concurrent.duration._

class StorageService[F[_]: Sync, V](expireAfterMinutes: Option[Int] = Some(240))
    extends StorageAlgebra[F, String, V] {
  private val lruCache: Cache[String, V] = {
    val cacheWithStats = Scaffeine().recordStats()

    val cache = expireAfterMinutes
      .map(mins => cacheWithStats.expireAfterAccess(mins.minutes))
      .getOrElse(cacheWithStats)

    cache.build[String, V]()
  }

  private val queueRef: Ref[F, Queue[V]] = Ref.unsafe[F, Queue[V]](Queue[V]())
  private val maxQueueSize = 20

  Metrics.cacheMetrics.addCache(this.getClass.getSimpleName, lruCache.underlying)

  def update(key: String, updateFunc: V => V, empty: => V): F[V] =
    lookup(key)
      .map(_.map(updateFunc).getOrElse(empty))
      .flatMap(put(key, _))

  def update(key: String, updateFunc: V => V): F[Option[V]] =
    lookup(key)
      .flatMap(_.map(updateFunc).traverse(put(key, _)))

  def put(key: String, value: V): F[V] =
    queueRef.get.flatMap { queue =>
      val dequeue = {
        if (queue.size == maxQueueSize) {
          queue.dequeue._2.pure[F]
        } else {
          queue.pure[F]
        }
      }

      dequeue *> queueRef
        .set(queue.enqueue(value))
        .flatTap { _ =>
          lruCache.put(key, value).pure[F]
        }
        .map(_ => value)
    }

  def lookup(key: String): F[Option[V]] =
    lruCache.getIfPresent(key).pure[F]

  def remove(keys: Set[String]): F[Unit] =
    lruCache.invalidateAll(keys).pure[F]

  def contains(key: String): F[Boolean] =
    lookup(key).map(_.isDefined)

  def size(): F[Long] = lruCache.estimatedSize().pure[F]

  def toMap(): F[Map[String, V]] =
    lruCache.asMap().toMap.pure[F]

  def getLast20(): F[List[V]] =
    queueRef.get.map(_.reverse.toList)
}
