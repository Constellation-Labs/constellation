package org.constellation.primitives.storage

trait Storage[F[_], K, V] {
  def getSync(key: K): Option[V]

  def get(key: K): F[Option[V]]

  def putSync(key: K, value: V): V

  def put(key: K, value: V): F[V]

  def updateSync(key: K, updateFunc: V => V, empty: => V): V

  def update(key: K, updateFunc: V => V, empty: => V): F[V]

  def update(key: K, updateFunc: V => V): F[Option[V]]

  def removeSync(key: K): Unit = removeSync(Set(key))

  def removeSync(keys: Set[K]): Unit

  def remove(key: K): F[Unit] = remove(Set(key))

  def remove(keys: Set[K]): F[Unit]

  def containsSync(key: K): Boolean

  def contains(key: K): F[Boolean]

  def toMapSync(): Map[K, V]

  def toMap(): F[Map[K, V]]

  def cacheSize(): Long
}
