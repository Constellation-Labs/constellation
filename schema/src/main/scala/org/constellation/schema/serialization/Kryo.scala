package org.constellation.schema.serialization

import cats.effect.Concurrent
import com.twitter.chill.{IKryoRegistrar, KryoPool, ScalaKryoInstantiator}

/*
  TODO: Consider making it just a class:

  class KryoSerializer[F[_]: Concurrent] extends Serializer {
    def addRegistrar(registrar: IKryoRegistrar) = ???

    def serializeAnyRef(anyRef: AnyRef): F[Array[Byte]] =
      kryoPool.toBytesWithClass(anyRef)

    def deserializeCast[T](bytes: Array[Byte]): F[T] =
      kryoPool.fromBytes(bytes).asInstanceOf[T]
  }
 */
object Kryo {

  private var kryoPool: KryoPool = _

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  def init[F[_]: Concurrent](registrar: IKryoRegistrar): F[Unit] =
    if (kryoPool != null) {
      Concurrent[F].raiseError(new Throwable("KryoPool has been already initiated."))
    } else {
      Concurrent[F].delay {
        val instance = new ScalaKryoInstantiator()
          .setRegistrationRequired(true)
          .withRegistrar(registrar)

        kryoPool = KryoPool.withByteArrayOutputStream(
          10,
          instance
        )
      }
    }

  def init[F[_]: Concurrent](): F[Unit] = init(SchemaKryoRegistrar)

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    kryoPool.fromBytes(bytes).asInstanceOf[T]
}
