package org.constellation.datastore

/** Documentation. */
trait KVDB {

  /** Documentation. */
  def put(key: String, obj: AnyRef): Boolean

  /** Documentation. */
  def get[T <: AnyRef](key: String): Option[T]

  /** Documentation. */
  def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T

  /** Documentation. */
  def delete(key: String): Boolean

  /** Documentation. */
  def restart(): Unit
}

