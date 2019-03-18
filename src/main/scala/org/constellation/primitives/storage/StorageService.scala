package org.constellation.primitives.storage

import cats.effect.IO
import com.twitter.storehaus.cache.MutableLRUCache

import scala.collection.JavaConverters._

class StorageService[V](size: Int = 50000) extends Storage[IO, String, V] with Lookup[String, V] {
  val lruCache: ExtendedMutableLRUCache[String, V] = new ExtendedMutableLRUCache[String, V](size)

  override def lookup(key: String): IO[Option[V]] = get(key)

  override def getSync(key: String): Option[V] =
    lruCache.get(key)

  override def putSync(key: String, value: V): V = {
    lruCache.+=((key, value))
    value
  }

  override def updateSync(key: String, updateFunc: V => V, empty: => V): V =
    putSync(key, getSync(key).map(updateFunc).getOrElse(empty))

  def updateOnly(key: String, updateFunc: V => V): Option[V] =
    getSync(key).map(updateFunc).map { putSync(key, _) }

  override def removeSync(keys: Set[String]): Unit =
    lruCache.multiRemove(keys)

  override def containsSync(key: String): Boolean =
    lruCache.contains(key)

  override def toMapSync(): Map[String, V] =
    lruCache.asImmutableMap()

  override def get(key: String): IO[Option[V]] =
    IO.pure(getSync(key))

  override def put(key: String, value: V): IO[V] =
    IO.pure(putSync(key, value))

  override def update(key: String, updateFunc: V => V, empty: => V): IO[V] =
    get(key)
      .map(_.map(updateFunc).getOrElse(empty))
      .flatMap(put(key, _))

  override def remove(keys: Set[String]): IO[Unit] =
    IO.pure(lruCache.multiRemove(keys))

  override def contains(key: String): IO[Boolean] =
    IO.pure(containsSync(key))

  override def toMap(): IO[Map[String, V]] =
    IO.pure(lruCache.iterator.toMap)
}

class ExtendedMutableLRUCache[K, V](capacity: Int) extends MutableLRUCache[K, V](capacity) {

  def asImmutableMap(): Map[K, V] = {
    m.asScala.toMap
  }

}
