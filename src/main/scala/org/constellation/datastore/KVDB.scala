package org.constellation.datastore

trait KVDB {
  def put(key: String, obj: AnyRef): Boolean
  def get[T <: AnyRef](key: String): Option[T]
  def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T
  def delete(key: String): Boolean
  def restart(): Unit
}
