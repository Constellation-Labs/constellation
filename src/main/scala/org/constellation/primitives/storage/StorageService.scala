package org.constellation.primitives.storage

import cats.effect.IO
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.twitter.storehaus.cache.MutableLRUCache

import scala.collection.JavaConverters._

//noinspection ScalaStyle
class StorageService[V](size: Int = 50000) extends Storage[IO, String, V] with Lookup[String, V] {
  val lruCache: Cache[String, V] =
    Scaffeine()
      .recordStats()
      .maximumSize(size)
      .build[String, V]()

  override def lookup(key: String): IO[Option[V]] = lruCache.synchronized { get(key) }

  override def getSync(key: String): Option[V] = lruCache.synchronized {
    lruCache.getIfPresent(key)
  }

  override def putSync(key: String, value: V): V = lruCache.synchronized {
    lruCache.put(key, value)
    value
  }

  override def updateSync(key: String, updateFunc: V => V, empty: => V): V = lruCache.synchronized {
    putSync(key, getSync(key).map(updateFunc).getOrElse(empty))
  }

  def updateOnly(key: String, updateFunc: V => V): Option[V] = lruCache.synchronized {
    getSync(key).map(updateFunc).map {
      putSync(key, _)
    }
  }

  override def removeSync(keys: Set[String]): Unit = lruCache.synchronized {
    lruCache.invalidateAll(keys)
  }

  override def containsSync(key: String): Boolean = lruCache.synchronized {
    lruCache.getIfPresent(key).isDefined
  }

  override def toMapSync(): Map[String, V] = lruCache.synchronized {
    lruCache.asMap().toMap
  }

  override def get(key: String): IO[Option[V]] = lruCache.synchronized {
    IO(getSync(key))
  }

  override def put(key: String, value: V): IO[V] = lruCache.synchronized {
    IO(putSync(key, value))
  }

  override def update(key: String, updateFunc: V => V, empty: => V): IO[V] = lruCache.synchronized {
    get(key)
      .map(_.map(updateFunc).getOrElse(empty))
      .flatMap(put(key, _))
  }

  override def remove(keys: Set[String]): IO[Unit] = lruCache.synchronized {
    IO(lruCache.invalidateAll(keys))
  }

  override def contains(key: String): IO[Boolean] = lruCache.synchronized {
    IO(containsSync(key))
  }

  override def toMap(): IO[Map[String, V]] = lruCache.synchronized {
    IO(lruCache.asMap().toMap)
  }

  override def cacheSize(): Long = lruCache.synchronized {
    lruCache.estimatedSize()
  }
}
