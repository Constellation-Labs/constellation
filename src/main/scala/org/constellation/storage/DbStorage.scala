package org.constellation.storage

import java.util.concurrent.ConcurrentLinkedQueue

import better.files.File
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.storage.algebra.LookupAlgebra
import swaydb.data.config.MMAP
import swaydb.persistent
import swaydb.serializers.Serializer

import scala.concurrent.ExecutionContext

abstract class DbStorage[K, V](dbPath: File)(implicit keySerializer: Serializer[K],
                                             valueSerializer: Serializer[V],
                                             contextShift: ContextShift[IO],
                                             ec: ExecutionContext)
    extends LookupAlgebra[IO, K, V] {

  implicit val bag =  swaydb.cats.effect.Bag(contextShift, ec)

  val db: swaydb.Map[K, V, Nothing, IO] =
    persistent.Map[K, V, Nothing, IO](
        dir = dbPath.path,
        mmapMaps = false,
        segmentConfig = persistent.DefaultConfigs.segmentConfig().copy(mmap = MMAP.Disabled),
        mmapAppendix = false
      ).unsafeRunSync()

  def lookup(key: K): IO[Option[V]] = db.get(key)

  def contains(key: K): IO[Boolean] = db.contains(key)

  def putSync(key: K, value: V): Unit = put(key, value).unsafeRunSync()

  def put(key: K, value: V): IO[Unit] = db.put(key, value).map(_ => ())

  def putAll(kvs: Iterable[(K, V)]): IO[Unit] = db.put(kvs).map(_ => ())

  def removeSync(key: K): Unit = remove(key).unsafeRunSync()

  def remove(key: K): IO[Unit] = db.remove(key).map(_ => ())

  def size: IO[Int] = db.sizeOfBloomFilterEntries
}

abstract class MidDbStorage[K, V](dbPath: File, capacity: Int)(
  implicit keySerializer: Serializer[K],
  valueSerializer: Serializer[V],
  contextShift: ContextShift[IO],
  ec: ExecutionContext
) extends DbStorage[K, V](dbPath) {

  private val hashQueue: ConcurrentLinkedQueue[K] = new ConcurrentLinkedQueue[K]()

  override def put(
    key: K,
    value: V
  ): IO[Unit] =
    super
      .put(key, value)
//      .flatTap(_ => IO(hashQueue.add(key)))

  override def putAll(kvs: Iterable[(K, V)]): IO[Unit] = {
    import scala.collection.JavaConverters._
    super
      .putAll(kvs)
//      .flatTap(_ => IO(hashQueue.addAll(kvs.map(_._1).asJavaCollection)))
  }

  def pullOverCapacity(): IO[List[V]] =
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

  private def poll(hash: K): IO[Option[V]] =
    lookup(hash)
      .flatTap(_ => remove(hash))

  override def remove(
    key: K
  ): IO[Unit] =
    super
      .remove(key)
      .flatTap(_ => IO(hashQueue.remove(key)))

  private def isOverCapacity: Boolean =
    hashQueue.size() >= capacity
}
