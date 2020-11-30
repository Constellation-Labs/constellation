package org.constellation.schema.serialization

import cats.effect.Concurrent
import com.twitter.chill.{IKryoRegistrar, KryoPool, ScalaKryoInstantiator}

object Kryo {

  private var kryoPool: KryoPool = _

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  def init[F[_]: Concurrent](registrar: IKryoRegistrar): F[Unit] = Concurrent[F].delay {
    if (kryoPool != null) {
      throw new Throwable("KryoPool has been already initiated.")
    } else {
      val instance = new ScalaKryoInstantiator()
        .setRegistrationRequired(true)
        .withRegistrar(registrar)

      kryoPool = KryoPool.withByteArrayOutputStream(
        10,
        instance
      )
    }
  }

  def init[F[_]: Concurrent](): F[Unit] = init(SingletonRegistrar)

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    kryoPool.fromBytes(bytes).asInstanceOf[T]
}
