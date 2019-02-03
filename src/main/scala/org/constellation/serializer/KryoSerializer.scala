package org.constellation.serializer

import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import org.constellation.p2p.SerializedUDPMessage

import scala.util.Random

/** Documentation. */
object KryoSerializer {

  /** Documentation. */
  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool: KryoPool = KryoPool.withBuffer(guessThreads,
    new ScalaKryoInstantiator().setRegistrationRequired(true)
      .withRegistrar(new ConstellationKryoRegistrar())
    , 32, 1024*1024*100)

  /** Documentation. */
  def serializeGrouped[T](data: T, groupSize: Int = 45000): Seq[SerializedUDPMessage] = {

    val bytes: Array[Byte] = kryoPool.toBytesWithClass(data)

    val idx: Seq[(Array[Byte], Int)] = bytes.grouped(groupSize).zipWithIndex.toSeq

    val pg: Long = Random.nextLong()

    idx.map { case (b: Array[Byte], i: Int) =>
      SerializedUDPMessage(ByteString(b),
        packetGroup = pg, packetGroupSize = idx.length, packetGroupId = i)
    }
  }

  /** Documentation. */
  def deserializeGrouped(messages: List[SerializedUDPMessage]): AnyRef = {
    val sortedBytes = messages.sortBy(f => f.packetGroupId).flatMap(_.data).toArray

    deserialize(sortedBytes)
  }

  /** Documentation. */
  def serialize[T](data: T): Array[Byte] = {
    kryoPool.toBytesWithClass(data)
  }

  // Use this one

  /** Documentation. */
  def serializeAnyRef(anyRef: AnyRef): Array[Byte] = {
    kryoPool.toBytesWithClass(anyRef)
  }

  /** Documentation. */
  def deserialize(message: Array[Byte]): AnyRef= {
    kryoPool.fromBytes(message)
  }

  // Use this one

  /** Documentation. */
  def deserializeCast[T](message: Array[Byte]): T = {
    kryoPool.fromBytes(message).asInstanceOf[T]
  }
/*

  /** Documentation. */
  def deserializeT[T : ClassTag](message: Array[Byte]): AnyRef= {

    val clz = {
      import scala.reflect._

      /** Documentation. */
      classTag[T].runtimeClass.asInstanceOf[Class[T]]
    }
    kryoPool.fromBytes(message, clz)
  }
*/

  /** Documentation. */
  def deserialize[T](message: Array[Byte], cls: Class[T]): T = {
    kryoPool.fromBytes(message, cls)
  }

}

