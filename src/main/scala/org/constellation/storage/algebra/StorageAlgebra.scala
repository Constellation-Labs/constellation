package org.constellation.storage.algebra

trait StorageAlgebra[F[_], K, V] extends LookupAlgebra[F, K, V] {

  def put(key: K, value: V): F[V]

  def update(key: K, updateFunc: V => V, empty: => V): F[V]

  def update(key: K, updateFunc: V => V): F[Option[V]]

  def remove(key: K): F[Unit] = remove(Set(key))

  def remove(keys: Set[K]): F[Unit]

  def size(): F[Long]

  def count(predicate: V => Boolean): F[Long]

  // Rethink if needed \/:

  def toMap(): F[Map[K, V]]

  def getLast20(): F[List[V]]

}
