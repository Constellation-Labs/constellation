package org.constellation.primitives.storage

trait Storage[F[_], K, V] {
  def get(key: K): Option[V]

  def getAsync(key: K): F[Option[V]]

  def put(key: K, value: V): V

  def putAsync(key: K, value: V): F[V]

  def update(key: K, updateFunc: V => V, empty: => V): V

  def updateAsync(key: K, updateFunc: V => V, empty: => V): F[V]

  def delete(key: K): Unit = delete(Set(key))

  def delete(keys: Set[K]): Unit

  def deleteAsync(key: K): F[Unit] = deleteAsync(Set(key))

  def deleteAsync(keys: Set[K]): F[Unit]

  def contains(key: K): Boolean

  def containsAsync(key: K): F[Boolean]

  def toMap(): Map[K, V]

  def toMapAsync(): F[Map[K, V]]
}
