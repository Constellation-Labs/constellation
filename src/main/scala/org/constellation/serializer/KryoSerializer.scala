package org.constellation.serializer

import java.io.{FileOutputStream, OutputStream}

import com.esotericsoftware.kryo.io.{Input, InputChunked, Output, OutputChunked}
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

import scala.util.Random

object KryoSerializer {

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool = KryoPool.withByteArrayOutputStream(
    10,
    new ScalaKryoInstantiator()
      .setRegistrationRequired(true)
      .withRegistrar(new ConstellationKryoRegistrar())
  )

  def serialize[T](data: T): Array[Byte] =
    kryoPool.toBytesWithClass(data)

  // Use this one

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  def deserialize(message: Array[Byte]): AnyRef =
    kryoPool.fromBytes(message)

  // Use this one

  def deserializeCast[T](message: Array[Byte]): T =
    kryoPool.fromBytes(message).asInstanceOf[T]
  /*

  def deserializeT[T : ClassTag](message: Array[Byte]): AnyRef= {

    val clz = {
      import scala.reflect._

      classTag[T].runtimeClass.asInstanceOf[Class[T]]
    }
    kryoPool.fromBytes(message, clz)
  }
   */

  def deserialize[T](message: Array[Byte], cls: Class[T]): T =
    kryoPool.fromBytes(message, cls)

}
