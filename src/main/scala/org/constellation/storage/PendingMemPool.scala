package org.constellation.storage

import org.constellation.storage.algebra.LookupAlgebra

trait PendingMemPool[F[_], A] extends LookupAlgebra[F, String, A] {
  def put(key: String, value: A): F[A]
  def update(key: String, fn: A => A): F[Option[A]]
  def pull(minCount: Int): F[Option[List[A]]]
  def size(): F[Long]
}
