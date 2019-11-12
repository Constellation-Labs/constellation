package org.constellation.serializer

trait Serializer {

  def serialize(anyRef: AnyRef): Array[Byte]

  def deserialize[T](message: Array[Byte]): T
}
