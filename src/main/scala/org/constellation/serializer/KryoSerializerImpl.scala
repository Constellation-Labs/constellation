package org.constellation.serializer

import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

class KryoSerializerImpl extends Serializer {

  private val guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  private val kryoPool: KryoPool = {
    KryoPool.withBuffer(
      guessThreads,
      new ScalaKryoInstantiator()
        .setRegistrationRequired(true)
        .withRegistrar(new ConstellationKryoRegistrar()),
      32,
      1024 * 1024 * 100
    )
  }

  override def serialize(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  override def deserialize[T](message: Array[Byte]): T =
    kryoPool.fromBytes(message).asInstanceOf[T]
}
