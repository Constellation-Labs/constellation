package org.constellation.datastore

/** Key-value database trait. */
trait KVDB {

  /** Put method. */
  def put(key: String, obj: AnyRef): Boolean

  /** Getter for the entry corresponding to input key. */
  def get[T <: AnyRef](key: String): Option[T]

  /** Update the entry corresponding to input key. */
  def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T

  /** Delete the entry corresponding to input key. */
  def delete(key: String): Boolean

  /** Reset. */
  def restart(): Unit
}
