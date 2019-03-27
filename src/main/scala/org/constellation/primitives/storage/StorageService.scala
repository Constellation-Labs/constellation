package org.constellation.primitives.storage

import cats.effect.IO
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.constellation.util.Metrics

//noinspection ScalaStyle
class StorageService[V](size: Int = 50000) extends Storage[IO, String, V] with Lookup[String, V] {
  private val lruCache: Cache[String, V] =
    Scaffeine()
      .recordStats()
      .maximumSize(size)
      .build[String, V]()

  Metrics.cacheMetrics.addCache(this.getClass.getSimpleName, lruCache.underlying)

  override def lookup(key: String): IO[Option[V]] = get(key)

  override def getSync(key: String): Option[V] =
    lruCache.getIfPresent(key)

  override def putSync(key: String, value: V): V = {
    lruCache.put(key, value)
    value
  }

  override def updateSync(key: String, updateFunc: V => V, empty: => V): V =
    putSync(key, getSync(key).map(updateFunc).getOrElse(empty))

  def updateOnly(key: String, updateFunc: V => V): Option[V] =
    getSync(key).map(updateFunc).map { putSync(key, _) }

  override def removeSync(keys: Set[String]): Unit =
    lruCache.invalidateAll(keys)

  override def containsSync(key: String): Boolean =
    lruCache.getIfPresent(key).isDefined

  override def toMapSync(): Map[String, V] =
    lruCache.asMap().toMap

  override def get(key: String): IO[Option[V]] =
    IO(getSync(key))

  override def put(key: String, value: V): IO[V] =
    IO(putSync(key, value))

  override def update(key: String, updateFunc: V => V, empty: => V): IO[V] =
    get(key)
      .map(_.map(updateFunc).getOrElse(empty))
      .flatMap(put(key, _))

  override def remove(keys: Set[String]): IO[Unit] =
    IO(lruCache.invalidateAll(keys))

  override def contains(key: String): IO[Boolean] =
    IO(containsSync(key))

  override def toMap(): IO[Map[String, V]] =
    IO(lruCache.asMap().toMap)

  override def cacheSize(): Long = lruCache.estimatedSize()
}
