package org.constellation.schema.serialization

import cats.effect.Concurrent
import com.twitter.chill.{IKryoRegistrar, KryoPool, ScalaKryoInstantiator}

object Kryo {

  private var kryoPool: KryoPool = _

  private var kryoHashingPool: KryoPool = _

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

        kryoHashingPool = KryoPool.withByteArrayOutputStream(
          10,
          instance
            .setReferences(false)
        )
      }
    }

  def init[F[_]: Concurrent](): F[Unit] = init(SchemaKryoRegistrar)

  /**
    * @param withRefs set to false to turn off reference tracking during serialization
    * @see <a href="https://github.com/EsotericSoftware/kryo#references">Kryo references</a>
    */
  def serializeAnyRef(anyRef: AnyRef, withRefs: Boolean = true): Array[Byte] =
    if (withRefs)
      kryoPool.toBytesWithClass(anyRef)
    else
      kryoHashingPool.toBytesWithClass(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    kryoPool.fromBytes(bytes).asInstanceOf[T]
}
