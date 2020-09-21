package org.constellation.serializer

import org.constellation.schema.Kryo

object KryoSerializer {

  val kryoPool = Kryo.init(new ConstellationKryoRegistrar())

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    kryoPool.fromBytes(bytes).asInstanceOf[T]
}
