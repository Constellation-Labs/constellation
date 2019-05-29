package org.constellation.primitives.storage

import java.util.concurrent.ConcurrentLinkedQueue

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.datastore.swaydb.SwayDbConversions._
import swaydb.data.config.MMAP
import swaydb.persistent
import swaydb.serializers.Serializer

trait Lookup[K, V] {
  def lookup(key: K): IO[Option[V]]
  def lookupSync(key: K): Option[V] = lookup(key).unsafeRunSync()

  def contains(key: K): IO[Boolean]
  def containsSync(key: K): Boolean = contains(key).unsafeRunSync()
}

abstract class DbStorage[K, V](dbPath: File)(implicit keySerializer: Serializer[K],
                                           valueSerializer: Serializer[V])
    extends Lookup[K, V] {

  val db: swaydb.Map[K, V] = persistent
    .Map[K, V](dir = dbPath.path,
               mmapMaps = false,
               mmapSegments = MMAP.Disabled,
               mmapAppendix = false)
    .get

  def lookup(key: K): IO[Option[V]] = db.get(key).asIO

  def contains(key: K): IO[Boolean] = db.contains(key).asIO

  def putSync(key: K, value: V): Unit = put(key, value).unsafeRunSync()

  def put(key: K, value: V): IO[Unit] = IO(db.put(key, value).asIO.get).map(_ => ())

  def removeSync(key: K): Unit = remove(key).unsafeRunSync()

  def remove(key: K): IO[Unit] = IO(db.remove(key).asIO.get).map(_ => ())
}

object DbStorage {

  def extendedLookup[K, V]: List[Lookup[K, V]] => K => IO[Option[V]] =
    (lookups: List[Lookup[K, V]]) =>
      (hash: K) =>
        lookups.foldLeft(IO.pure[Option[V]](None)) {
          case (io, memPool) =>
            io.flatMap { o =>
              o.fold(memPool.lookup(hash))(b => IO.pure(Some(b)))
            }
    }

  def extendedContains[K, V]: List[Lookup[K, V]] ⇒ K ⇒ IO[Boolean] =
    (lookups: List[Lookup[K, V]]) ⇒
      (hash: K) ⇒
        lookups.foldLeft(IO.pure[Boolean](false)) {
          case (io, memPool) ⇒
            io.flatMap { o ⇒
              if (o)
                IO.pure(o)
              else
                memPool.contains(hash)
            }
        }
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
    //  .flatTap(_ => IO(hashQueue.add(key)))

  def pullOverCapacity(): IO[List[V]] = {
    if (!isOverCapacity) {
      return IO.pure(List())
    }

    var hashes = List[K]()

    while (isOverCapacity) {
      hashes = hashes :+ hashQueue.poll()
    }

    hashes
      .map(poll)
      .sequence[IO, Option[V]]
      .map(_.flatten)
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
