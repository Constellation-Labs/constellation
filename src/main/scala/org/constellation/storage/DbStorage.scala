package org.constellation.storage

import java.util.concurrent.ConcurrentLinkedQueue

import better.files.File
import cats.Applicative
import cats.effect.{IO, Sync}
import cats.implicits._
import org.constellation.datastore.swaydb.SwayDbConversions._
import org.constellation.storage.algebra.LookupAlgebra
import swaydb.data.config.MMAP
import swaydb.persistent
import swaydb.serializers.Serializer

abstract class DbStorage[K, V](dbPath: File)(implicit keySerializer: Serializer[K],
                                           valueSerializer: Serializer[V])
    extends LookupAlgebra[IO, K, V] {

  val db: swaydb.Map[K, V] = persistent
    .Map[K, V](dir = dbPath.path,
               mmapMaps = false,
               mmapSegments = MMAP.Disabled,
               mmapAppendix = false)
    .get

  def lookup(key: K): IO[Option[V]] = db.get(key)

  def contains(key: K): IO[Boolean] = db.contains(key)

  def putSync(key: K, value: V): Unit = put(key, value).unsafeRunSync()

  def put(key: K, value: V): IO[Unit] = IO(db.put(key, value).get).map(_ => ())

  def putAll(kvs: Iterable[(K, V)]): IO[Unit] = IO(db.put(kvs))

  def removeSync(key: K): Unit = remove(key).unsafeRunSync()

  def remove(key: K): IO[Unit] = db.remove(key).map(_ => ())

  def size: Int = db.size
}

abstract class MidDbStorage[K, V](dbPath: File, capacity: Int)(implicit keySerializer: Serializer[K],
                                                             valueSerializer: Serializer[V])
    extends DbStorage[K, V](dbPath) {

  private val hashQueue: ConcurrentLinkedQueue[K] = new ConcurrentLinkedQueue[K]()

  override def put(
    key: K,
    value: V
  ): IO[Unit] =
    super
      .put(key, value)
//      .flatTap(_ => IO(hashQueue.add(key)))

  override def putAll(kvs: Iterable[(K,V)]): IO[Unit] = {
    import scala.collection.JavaConverters._
    super
      .putAll(kvs)
//      .flatTap(_ => IO(hashQueue.addAll(kvs.map(_._1).asJavaCollection)))
  }

  def pullOverCapacity(): IO[List[V]] = {
    if (!isOverCapacity) {
      IO.pure(Nil)
    } else {

      var hashes = List[K]()

      while (isOverCapacity) {
        hashes = hashes :+ hashQueue.poll()
      }

      hashes
        .map(poll)
        .sequence[IO, Option[V]]
        .map(_.flatten)
    }
  }

  private def poll(hash: K): IO[Option[V]] = {
    lookup(hash)
      .flatTap(_ => remove(hash))
  }

  override def remove(
    key: K
  ): IO[Unit] =
    super
      .remove(key)
      .flatTap(_ => IO(hashQueue.remove(key)))

  private def isOverCapacity: Boolean =
    hashQueue.size() >= capacity
}
