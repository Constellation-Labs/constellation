package org.constellation.primitives.storage

import cats.effect.IO
import com.twitter.storehaus.cache.MutableLRUCache

class StorageService[V](size: Int = 50000) extends Storage[IO, String, V] {
  val lruCache: MutableLRUCache[String, V] = MutableLRUCache[String, V](size)

  override def get(key: String): Option[V] =
    lruCache.get(key)

  override def put(key: String, value: V): V = {
    lruCache.+=((key, value))
    value
  }

  override def update(key: String, updateFunc: V => V, empty: => V): V =
    put(key, get(key).map(updateFunc).getOrElse(empty))

  def updateOnly(key: String, updateFunc: V => V): Option[V] =
    get(key).map(updateFunc).map{put(key, _)}

  override def delete(keys: Set[String]): Unit =
    lruCache.multiRemove(keys)

  override def contains(key: String): Boolean =
    lruCache.contains(key)

  override def toMap(): Map[String, V] =
    // TODO: wkoszycki replace with ExtendedLRUCache
    this.synchronized {
      lruCache.iterator.toMap
    }

  override def getAsync(key: String): IO[Option[V]] =
    IO.pure(get(key))

  override def putAsync(key: String, value: V): IO[V] =
    IO.pure(put(key, value))

  override def updateAsync(key: String, updateFunc: V => V, empty: => V): IO[V] =
    getAsync(key)
      .map(_.map(updateFunc).getOrElse(empty))
      .flatMap(putAsync(key, _))

  override def deleteAsync(keys: Set[String]): IO[Unit] =
    IO.pure(lruCache.multiRemove(keys))

  override def containsAsync(key: String): IO[Boolean] =
    IO.pure(contains(key))

  override def toMapAsync(): IO[Map[String, V]] =
    IO.pure(lruCache.iterator.toMap)
}
