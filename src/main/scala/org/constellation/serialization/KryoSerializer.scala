package org.constellation.serialization

import cats.Monoid
import cats.effect.Concurrent
import org.constellation.schema.serialization.ExplicitKryoRegistrar._
import org.constellation.schema.serialization.{ExplicitKryoRegistrar, Kryo, SchemaKryoRegistrar}

object KryoSerializer {

  def init[F[_]: Concurrent]: F[Unit] = Kryo.init(
    Monoid[ExplicitKryoRegistrar].combine(
      SchemaKryoRegistrar,
      ConstellationKryoRegistrar
    )
  )

  def serializeAnyRef(anyRef: AnyRef, withRefs: Boolean = true): Array[Byte] =
    Kryo.serializeAnyRef(anyRef, withRefs)

  def deserializeCast[T](bytes: Array[Byte]): T =
    Kryo.deserializeCast(bytes)
}
