package org.constellation.serializer

import cats.Monoid
import cats.effect.Concurrent
import org.constellation.schema.serialization.{ExplicitKryoRegistrar, Kryo, SingletonRegistrar => SchemaRegistrar}
import org.constellation.schema.serialization.ExplicitKryoRegistrar._

object KryoSerializer {

  def init[F[_]: Concurrent]: F[Unit] = Kryo.init(
    Monoid[ExplicitKryoRegistrar].combine(
      SchemaRegistrar,
      ConstellationKryoRegistrar
    )
  )

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    Kryo.serializeAnyRef(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    Kryo.deserializeCast(bytes)
}
